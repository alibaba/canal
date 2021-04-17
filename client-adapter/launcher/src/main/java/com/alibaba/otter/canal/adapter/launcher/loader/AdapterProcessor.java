package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.adapter.launcher.common.SyncSwitch;
import com.alibaba.otter.canal.adapter.launcher.config.SpringContext;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.MessageUtil;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.connector.core.config.CanalConstants;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.spi.CanalMsgConsumer;
import com.alibaba.otter.canal.connector.core.spi.ExtensionLoader;

/**
 * 适配处理器
 * 
 * @author rewerma 2020-02-01
 * @version 1.0.0
 */
public class AdapterProcessor {

    private static final Logger             logger                    = LoggerFactory.getLogger(AdapterProcessor.class);

    private static final String             CONNECTOR_SPI_DIR         = "/plugin";
    private static final String             CONNECTOR_STANDBY_SPI_DIR = "/canal-adapter/plugin";

    private CanalMsgConsumer                canalMsgConsumer;

    private String                          canalDestination;                                                           // canal实例
    private String                          groupId                   = null;                                           // groupId
    private List<List<OuterAdapter>>        canalOuterAdapters;                                                         // 外部适配器
    private CanalClientConfig               canalClientConfig;                                                          // 配置
    private ExecutorService                 groupInnerExecutorService;                                                  // 组内工作线程池
    private volatile boolean                running                   = false;                                          // 是否运行中
    private Thread                          thread                    = null;
    private Thread.UncaughtExceptionHandler handler                   = (t, e) -> logger
        .error("parse events has an error", e);

    private SyncSwitch                      syncSwitch;

    public AdapterProcessor(CanalClientConfig canalClientConfig, String destination, String groupId,
                            List<List<OuterAdapter>> canalOuterAdapters){
        this.canalClientConfig = canalClientConfig;
        this.canalDestination = destination;
        this.groupId = groupId;
        this.canalOuterAdapters = canalOuterAdapters;

        this.groupInnerExecutorService = Util.newFixedThreadPool(canalOuterAdapters.size(), 5000L);
        syncSwitch = (SyncSwitch) SpringContext.getBean(SyncSwitch.class);

        // load connector consumer
        ExtensionLoader<CanalMsgConsumer> loader = new ExtensionLoader<>(CanalMsgConsumer.class);
        canalMsgConsumer = loader
            .getExtension(canalClientConfig.getMode().toLowerCase(), CONNECTOR_SPI_DIR, CONNECTOR_STANDBY_SPI_DIR);

        Properties properties = canalClientConfig.getConsumerProperties();
        properties.put(CanalConstants.CANAL_MQ_FLAT_MESSAGE, canalClientConfig.getFlatMessage());
        properties.put(CanalConstants.CANAL_ALIYUN_ACCESS_KEY, canalClientConfig.getAccessKey());
        properties.put(CanalConstants.CANAL_ALIYUN_SECRET_KEY, canalClientConfig.getSecretKey());
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(canalMsgConsumer.getClass().getClassLoader());
        canalMsgConsumer.init(properties, canalDestination, groupId);
        Thread.currentThread().setContextClassLoader(cl);
    }

    public void start() {
        if (!running) {
            thread = new Thread(this::process);
            thread.setUncaughtExceptionHandler(handler);
            thread.start();
            running = true;
        }
    }

    public void writeOut(final List<CommonMessage> commonMessages) {
        List<Future<Boolean>> futures = new ArrayList<>();
        // 组间适配器并行运行
        canalOuterAdapters.forEach(outerAdapters -> {
            futures.add(groupInnerExecutorService.submit(() -> {
                try {
                    // 组内适配器穿行运行，尽量不要配置组内适配器
                    outerAdapters.forEach(adapter -> {
                        long begin = System.currentTimeMillis();
                        List<Dml> dmls = MessageUtil.flatMessage2Dml(canalDestination, groupId, commonMessages);
                        batchSync(dmls, adapter);

                        if (logger.isDebugEnabled()) {
                            logger.debug("{} elapsed time: {}",
                                adapter.getClass().getName(),
                                (System.currentTimeMillis() - begin));
                        }
                    });
                    return true;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            }));

            // 等待所有适配器写入完成
            // 由于是组间并发操作，所以将阻塞直到耗时最久的工作组操作完成
            RuntimeException exception = null;
            for (Future<Boolean> future : futures) {
                try {
                    if (!future.get()) {
                        exception = new RuntimeException("Outer adapter sync failed! ");
                    }
                } catch (Exception e) {
                    exception = new RuntimeException(e);
                }
            }
            if (exception != null) {
                throw exception;
            }
        });
    }

    /**
     * 分批同步
     *
     * @param dmls
     * @param adapter
     */
    private void batchSync(List<Dml> dmls, OuterAdapter adapter) {
        // 分批同步
        if (dmls.size() <= canalClientConfig.getSyncBatchSize()) {
            adapter.sync(dmls);
        } else {
            int len = 0;
            List<Dml> dmlsBatch = new ArrayList<>();
            for (Dml dml : dmls) {
                dmlsBatch.add(dml);
                if (dml.getData() == null || dml.getData().isEmpty()) {
                    len += 1;
                } else {
                    len += dml.getData().size();
                }
                if (len >= canalClientConfig.getSyncBatchSize()) {
                    adapter.sync(dmlsBatch);
                    dmlsBatch.clear();
                    len = 0;
                }
            }
            if (!dmlsBatch.isEmpty()) {
                adapter.sync(dmlsBatch);
            }
        }
    }

    private void process() {
        while (!running) { // waiting until running == true
            while (!running) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }

        int retry = canalClientConfig.getRetries() == null
                    || canalClientConfig.getRetries() == 0 ? 1 : canalClientConfig.getRetries();
        if (retry == -1) {
            // 重试次数-1代表异常时一直阻塞重试
            retry = Integer.MAX_VALUE;
        }

        while (running) {
            try {
                syncSwitch.get(canalDestination);

                logger.info("=============> Start to connect destination: {} <=============", this.canalDestination);
                canalMsgConsumer.connect();
                logger.info("=============> Subscribe destination: {} succeed <=============", this.canalDestination);
                while (running) {
                    try {
                        syncSwitch.get(canalDestination, 1L, TimeUnit.MINUTES);
                    } catch (TimeoutException e) {
                        break;
                    }
                    if (!running) {
                        break;
                    }

                    for (int i = 0; i < retry; i++) {
                        if (!running) {
                            break;
                        }
                        try {
                            if (logger.isDebugEnabled()) {
                                logger.debug("destination: {} ", canalDestination);
                            }
                            long begin = System.currentTimeMillis();
                            List<CommonMessage> commonMessages = canalMsgConsumer
                                .getMessage(this.canalClientConfig.getTimeout(), TimeUnit.MILLISECONDS);
                            writeOut(commonMessages);
                            canalMsgConsumer.ack();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destination: {} elapsed time: {} ms",
                                    canalDestination,
                                    System.currentTimeMillis() - begin);
                            }
                            break;
                        } catch (Exception e) {
                            if (i != retry - 1) {
                                canalMsgConsumer.rollback(); // 处理失败, 回滚数据
                                logger.error(e.getMessage() + " Error sync and rollback, execute times: " + (i + 1));
                            } else {
                                canalMsgConsumer.ack();
                                logger.error(e.getMessage() + " Error sync but ACK!");
                            }
                            Thread.sleep(500);
                        }
                    }
                }

                canalMsgConsumer.disconnect();
            } catch (Throwable e) {
                logger.error("process error!", e);
            }

            if (running) { // is reconnect
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    public void stop() {
        try {
            if (!running) {
                return;
            }

            running = false;

            syncSwitch.release(canalDestination);

            logger.info("destination {} is waiting for adapters' worker thread die!", canalDestination);
            if (thread != null) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            groupInnerExecutorService.shutdown();
            logger.info("destination {} adapters worker thread dead!", canalDestination);
            canalOuterAdapters.forEach(outerAdapters -> outerAdapters.forEach(OuterAdapter::destroy));
            logger.info("destination {} all adapters destroyed!", canalDestination);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}

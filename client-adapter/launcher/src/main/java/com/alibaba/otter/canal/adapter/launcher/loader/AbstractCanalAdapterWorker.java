package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.adapter.launcher.common.SyncSwitch;
import com.alibaba.otter.canal.adapter.launcher.config.SpringContext;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.MessageUtil;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 适配器工作线程抽象类
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public abstract class AbstractCanalAdapterWorker {

    protected final Logger                    logger  = LoggerFactory.getLogger(this.getClass());

    protected String                          canalDestination;                                                // canal实例
    protected String                          groupId = null;                                                  // groupId
    protected List<List<OuterAdapter>>        canalOuterAdapters;                                              // 外部适配器
    protected CanalClientConfig               canalClientConfig;                                               // 配置
    protected ExecutorService                 groupInnerExecutorService;                                       // 组内工作线程池
    protected volatile boolean                running = false;                                                 // 是否运行中
    protected Thread                          thread  = null;
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    protected SyncSwitch                      syncSwitch;

    public AbstractCanalAdapterWorker(List<List<OuterAdapter>> canalOuterAdapters){
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Util.newFixedThreadPool(canalOuterAdapters.size(), 5000L);
        syncSwitch = (SyncSwitch) SpringContext.getBean(SyncSwitch.class);
    }

    protected void writeOut(final Message message) {
        List<Future<Boolean>> futures = new ArrayList<>();
        // 组间适配器并行运行
        canalOuterAdapters.forEach(outerAdapters -> {
            final List<OuterAdapter> adapters = outerAdapters;
            futures.add(groupInnerExecutorService.submit(() -> {
                try {
                    // 组内适配器穿行运行，尽量不要配置组内适配器
                    adapters.forEach(adapter -> {
                        long begin = System.currentTimeMillis();
                        List<Dml> dmls = MessageUtil.parse4Dml(canalDestination, groupId, message);
                        if (dmls != null) {
                            batchSync(dmls, adapter);

                            if (logger.isDebugEnabled()) {
                                logger.debug("{} elapsed time: {}",
                                    adapter.getClass().getName(),
                                    (System.currentTimeMillis() - begin));
                            }
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

    protected void writeOut(final List<FlatMessage> flatMessages) {
        List<Future<Boolean>> futures = new ArrayList<>();
        // 组间适配器并行运行
        canalOuterAdapters.forEach(outerAdapters -> {
            futures.add(groupInnerExecutorService.submit(() -> {
                try {
                    // 组内适配器穿行运行，尽量不要配置组内适配器
                    outerAdapters.forEach(adapter -> {
                        long begin = System.currentTimeMillis();
                        List<Dml> dmls = MessageUtil.flatMessage2Dml(canalDestination, groupId, flatMessages);
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

    @SuppressWarnings("unchecked")
    protected boolean mqWriteOutData(int retry, long timeout, int i, final boolean flatMessage,
                                     CanalMQConnector connector, ExecutorService workerExecutor) {
        try {
            List<?> messages;
            if (!flatMessage) {
                messages = connector.getListWithoutAck(100L, TimeUnit.MILLISECONDS);
            } else {
                messages = connector.getFlatListWithoutAck(100L, TimeUnit.MILLISECONDS);
            }
            if (messages != null && !messages.isEmpty()) {
                Future<Boolean> future = workerExecutor.submit(() -> {
                    if (flatMessage) {
                        // batch write
                        writeOut((List<FlatMessage>) messages);
                        // FIXME xxx
                        // messages.forEach((message ->
                        // System.out.println(JSON.toJSONString(message))));
                    } else {
                        for (final Object message : messages) {
                            writeOut((Message) message);
                        }
                    }
                    return true;
                });

                try {
                    future.get(timeout, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    future.cancel(true);
                    throw e;
                }
                connector.ack();
            }
            return true;
        } catch (Throwable e) {
            if (i == retry - 1) {
                connector.ack();
                logger.error(e.getMessage() + " Error sync but ACK!");
                return true;
            } else {
                connector.rollback();
                logger.error(e.getMessage() + " Error sync and rollback, execute times: " + (i + 1));
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e1) {
                // ignore
            }
        }
        return false;
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

    public void start() {
        if (!running) {
            thread = new Thread(this::process);
            thread.setUncaughtExceptionHandler(handler);
            thread.start();
            running = true;
        }
    }

    protected abstract void process();

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

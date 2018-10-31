package com.alibaba.otter.canal.adapter.launcher.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.adapter.launcher.common.SyncSwitch;
import com.alibaba.otter.canal.adapter.launcher.config.SpringContext;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.MessageUtil;
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
    protected List<List<OuterAdapter>>        canalOuterAdapters;                                              // 外部适配器
    protected ExecutorService                 groupInnerExecutorService;                                       // 组内工作线程池
    protected volatile boolean                running = false;                                                 // 是否运行中
    protected Thread                          thread  = null;
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    protected SyncSwitch                      syncSwitch;

    public AbstractCanalAdapterWorker(List<List<OuterAdapter>> canalOuterAdapters){
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
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
                        MessageUtil.parse4Dml(canalDestination, message, adapter::sync);
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} elapsed time: {}",
                                adapter.getClass().getName(),
                                (System.currentTimeMillis() - begin));
                        }
                    });
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }));

            // 等待所有适配器写入完成
            // 由于是组间并发操作，所以将阻塞直到耗时最久的工作组操作完成
            futures.forEach(future -> {
                try {
                    if (!future.get()) {
                        logger.error("Outer adapter write failed");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    // ignore
                }
            });
        });
    }

    protected void writeOut(final FlatMessage flatMessage) {
        List<Future<Boolean>> futures = new ArrayList<>();
        // 组间适配器并行运行
        canalOuterAdapters.forEach(outerAdapters -> {
            futures.add(groupInnerExecutorService.submit(() -> {
                try {
                    // 组内适配器穿行运行，尽量不要配置组内适配器
                    outerAdapters.forEach(adapter -> {
                        long begin = System.currentTimeMillis();
                        Dml dml = MessageUtil.flatMessage2Dml(canalDestination, flatMessage);
                        adapter.sync(dml);
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} elapsed time: {}",
                                adapter.getClass().getName(),
                                (System.currentTimeMillis() - begin));
                        }
                    });
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }));

            // 等待所有适配器写入完成
            // 由于是组间并发操作，所以将阻塞直到耗时最久的工作组操作完成
            futures.forEach(future -> {
                try {
                    if (!future.get()) {
                        logger.error("Outer adapter write failed");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    // ignore
                }
            });
        });
    }

    protected void mqWriteOutData(int retry, long timeout, boolean flatMessage, CanalMQConnector connector,
                        ExecutorService workerExecutor) {
        for (int i = 0; i < retry; i++) {
            try {
                List<?> messages;
                if (!flatMessage) {
                    messages = connector.getListWithoutAck(100L, TimeUnit.MILLISECONDS);
                } else {
                    messages = connector.getFlatListWithoutAck(100L, TimeUnit.MILLISECONDS);
                }
                if (messages != null) {
                    Future<Boolean> future = workerExecutor.submit(() -> {
                        for (final Object message : messages) {
                            if (message instanceof FlatMessage) {
                                writeOut((FlatMessage) message);
                            } else {
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
                }
                connector.ack();
                break;
            } catch (Throwable e) {
                if (i == retry - 1) {
                    connector.ack();
                } else {
                    connector.rollback();
                }

                logger.error(e.getMessage(), e);
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e1) {
                    // ignore
                }
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

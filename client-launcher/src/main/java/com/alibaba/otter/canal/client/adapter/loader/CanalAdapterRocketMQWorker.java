package com.alibaba.otter.canal.client.adapter.loader;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnectorProvider;
import com.alibaba.otter.canal.protocol.Message;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.WakeupException;

/**
 * kafka对应的client适配器工作线程
 *
 * @author machengyuan 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class CanalAdapterRocketMQWorker extends AbstractCanalAdapterWorker {

    private RocketMQCanalConnector connector;

    private String topic;

    public CanalAdapterRocketMQWorker(String nameServers, String topic, String groupId,
        List<List<CanalOuterAdapter>> canalOuterAdapters) {
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        this.topic = topic;
        this.canalDestination = topic;
        connector = RocketMQCanalConnectorProvider.newRocketMQConnector(nameServers, topic, groupId);
    }

    @Override
    public void start() {
        if (!running) {
            thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    process();
                }
            });
            thread.setUncaughtExceptionHandler(handler);
            running = true;
            thread.start();
        }
    }

    @Override
    public void stop() {
        try {
            if (!running) {
                return;
            }

            connector.stopRunning();
            running = false;

            // if (switcher != null && !switcher.state()) {
            // switcher.set(true);
            // }

            if (thread != null) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            groupInnerExecutorService.shutdown();
            logger.info("topic {} connectors' worker thread dead!", this.topic);
            for (List<CanalOuterAdapter> outerAdapters : canalOuterAdapters) {
                for (CanalOuterAdapter adapter : outerAdapters) {
                    adapter.destroy();
                }
            }
            logger.info("topic {} all connectors destroyed!", this.topic);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void process() {
        while (!running)
            ;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean executing = new AtomicBoolean(true);
        while (running) {
            try {
                logger.info("=============> Start to connect topic: {} <=============", this.topic);
                connector.connect();
                logger.info("=============> Start to subscribe topic: {}<=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed<=============", this.topic);
                while (running) {
                    try {
                        // switcher.get(); //等待开关开启

                        final Message message = connector.getWithoutAck(1);

                        executing.set(true);
                        if (message != null) {
                            executor.submit(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("topic: {} batchId: {} batchSize: {} ",
                                                topic,
                                                message.getId(),
                                                message.getEntries().size());
                                        }
                                        long begin = System.currentTimeMillis();
                                        writeOut(message);
                                        long now = System.currentTimeMillis();
                                        if ((System.currentTimeMillis() - begin) > 5 * 60 * 1000) {
                                            logger.error("topic: {} batchId {} elapsed time: {} ms",
                                                topic,
                                                message.getId(),
                                                now - begin);
                                        }
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("topic: {} batchId {} elapsed time: {} ms",
                                                topic,
                                                message.getId(),
                                                now - begin);
                                        }
                                        connector.ack(message.getId());
                                    } catch (Exception e) {
                                        logger.error(e.getMessage(), e);
                                    } finally {
                                        executing.compareAndSet(true, false);
                                    }
                                }
                            });
                        } else {
                            logger.debug("Message is null");
                        }
                    } catch (CommitFailedException e) {
                        logger.warn(e.getMessage());
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        TimeUnit.SECONDS.sleep(1L);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        executor.shutdown();

        try {
            connector.unsubscribe();
        } catch (WakeupException e) {
            // No-op. Continue process
        }
        connector.stopRunning();
        logger.info("=============> Disconnect topic: {} <=============", this.topic);
    }
}

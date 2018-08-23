package com.alibaba.otter.canal.client.adapter.loader;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.adapter.loader.AbstractCanalAdapterWorker;
import com.alibaba.otter.canal.kafka.client.KafkaCanalConnector;
import com.alibaba.otter.canal.kafka.client.KafkaCanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CanalAdapterKafkaWorker extends AbstractCanalAdapterWorker {

    private KafkaCanalConnector connector;

    private String              topic;

    public CanalAdapterKafkaWorker(String zkServers, String bootstrapServers, String topic, String groupId,
                                   List<List<CanalOuterAdapter>> canalOuterAdapters){
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        this.topic = topic;
        this.canalDestination = topic;
        connector = KafkaCanalConnectors.newKafkaConnector(zkServers, bootstrapServers, topic, null, groupId);
        // connector.setSessionTimeout(5L, TimeUnit.MINUTES);

        // super.initSwitcher(topic);
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
        ExecutorService executor = Executors.newFixedThreadPool(1);
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

                        final Message message = connector.getWithoutAck();

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
                                    } catch (Exception e) {
                                        logger.error(e.getMessage(), e);
                                    } finally {
                                        executing.compareAndSet(true, false);
                                    }
                                }
                            });

                            while (executing.get()) { // keeping kafka client active
                                connector.ack();
                                Thread.sleep(500);
                            }
                        } else {
                            connector.ack();
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
        connector.disconnnect();
        logger.info("=============> Disconnect topic: {} <=============", this.topic);
    }
}

package com.alibaba.otter.canal.client.adapter.loader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.WakeupException;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnectors;
import com.alibaba.otter.canal.protocol.Message;

/**
 * kafka对应的client适配器工作线程
 *
 * @author machengyuan 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class CanalAdapterKafkaWorker extends AbstractCanalAdapterWorker {

    private KafkaCanalConnector connector;

    private String topic;

    public CanalAdapterKafkaWorker(String zkServers, String bootstrapServers, String topic, String groupId,
        List<List<CanalOuterAdapter>> canalOuterAdapters) {
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
            logger.info("Stop topic {} out adapters begin", this.topic);
            stopOutAdapters();
            logger.info("Stop topic {} out adapters end", this.topic);
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

                        final Message message = connector.getWithoutAck();

                        executing.set(true);
                        if (message != null) {
                            executor.submit(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        writeOut(message, topic);
                                    } catch (Exception e) {
                                        logger.error(e.getMessage(), e);
                                    } finally {
                                        executing.compareAndSet(true, false);
                                    }
                                }
                            });

                            // 间隔一段时间ack一次, 防止因超时未响应切换到另外台客户端
                            while (executing.get()) {
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

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
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * kafka对应的client适配器工作线程
 *
 * @author machengyuan 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class CanalAdapterKafkaWorker extends AbstractCanalAdapterWorker {

    private KafkaCanalConnector connector;

    private String              topic;

    private boolean             flatMessage;

    public CanalAdapterKafkaWorker(String bootstrapServers, String topic, String groupId,
                                   List<List<CanalOuterAdapter>> canalOuterAdapters, boolean flatMessage){
        this.canalOuterAdapters = canalOuterAdapters;
        this.groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        this.topic = topic;
        this.canalDestination = topic;
        this.flatMessage = flatMessage;
        connector = KafkaCanalConnectors.newKafkaConnector(bootstrapServers, topic, null, groupId, flatMessage);
        // connector.setSessionTimeout(1L, TimeUnit.MINUTES);

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
        ExecutorService executor = Executors.newSingleThreadExecutor();
        // final AtomicBoolean executing = new AtomicBoolean(true);
        while (running) {
            try {
                logger.info("=============> Start to connect topic: {} <=============", this.topic);
                connector.connect();
                logger.info("=============> Start to subscribe topic: {} <=============", this.topic);
                connector.subscribe();
                logger.info("=============> Subscribe topic: {} succeed <=============", this.topic);
                while (running) {
                    try {
                        // switcher.get(); //等待开关开启

                        List<?> messages;
                        if (!flatMessage) {
                            messages = connector.getWithoutAck();
                        } else {
                            messages = connector.getFlatMessageWithoutAck(100L, TimeUnit.MILLISECONDS);
                        }
                        if (messages != null) {
                            for (final Object message : messages) {
                                if (message instanceof FlatMessage) {
                                    writeOut((FlatMessage) message);
                                } else {
                                    writeOut((Message) message);
                                }
                                // executing.set(true);
                                // if (message != null) {
                                // executor.submit(new Runnable() {
                                //
                                // @Override
                                // public void run() {
                                // try {
                                // if (message instanceof FlatMessage) {
                                // writeOut((FlatMessage) message);
                                // } else {
                                // writeOut((Message) message);
                                // }
                                // } catch (Exception e) {
                                // logger.error(e.getMessage(), e);
                                // } finally {
                                // executing.compareAndSet(true, false);
                                // }
                                // }
                                // });
                                //
                                // // 间隔一段时间ack一次, 防止因超时未响应切换到另外台客户端
                                // long currentTS = System.currentTimeMillis();
                                // while (executing.get()) {
                                // // 大于10秒未消费完ack一次keep alive
                                // if (System.currentTimeMillis() - currentTS > 10000) {
                                // connector.ack();
                                // currentTS = System.currentTimeMillis();
                                // }
                                // }
                                // }
                            }
                        }
                        connector.ack();
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
        connector.disconnect();
        logger.info("=============> Disconnect topic: {} <=============", this.topic);
    }
}

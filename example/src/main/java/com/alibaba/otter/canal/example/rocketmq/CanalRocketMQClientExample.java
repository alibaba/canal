package com.alibaba.otter.canal.example.rocketmq;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.example.kafka.AbstractKafkaTest;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Kafka client example
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class CanalRocketMQClientExample extends AbstractRocektMQTest {

    protected final static Logger           logger  = LoggerFactory.getLogger(CanalRocketMQClientExample.class);

    private RocketMQCanalConnector          connector;

    private static volatile boolean         running = false;

    private Thread                          thread  = null;

    private Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

                                                        public void uncaughtException(Thread t, Throwable e) {
                                                            logger.error("parse events has an error", e);
                                                        }
                                                    };

    public CanalRocketMQClientExample(String nameServers, String topic, String groupId){
        connector = new RocketMQCanalConnector(nameServers, topic, groupId, 500, false);
    }

    public static void main(String[] args) {
        try {
            final CanalRocketMQClientExample rocketMQClientExample = new CanalRocketMQClientExample(nameServers,
                topic,
                groupId);
            logger.info("## Start the rocketmq consumer: {}-{}", AbstractKafkaTest.topic, AbstractKafkaTest.groupId);
            rocketMQClientExample.start();
            logger.info("## The canal rocketmq consumer is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## Stop the rocketmq consumer");
                        rocketMQClientExample.stop();
                    } catch (Throwable e) {
                        logger.warn("## Something goes wrong when stopping rocketmq consumer:", e);
                    } finally {
                        logger.info("## Rocketmq consumer is down.");
                    }
                }

            });
            while (running)
                ;
        } catch (Throwable e) {
            logger.error("## Something going wrong when starting up the rocketmq consumer:", e);
            System.exit(0);
        }
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    List<Message> messages = connector.getListWithoutAck(100L, TimeUnit.MILLISECONDS); // 获取message
                    for (Message message : messages) {
                        long batchId = message.getId();
                        int size = message.getEntries().size();
                        if (batchId == -1 || size == 0) {
                            // try {
                            // Thread.sleep(1000);
                            // } catch (InterruptedException e) {
                            // }
                        } else {
                            printSummary(message, batchId, size);
                            printEntry(message.getEntries());
                            // logger.info(message.toString());
                        }
                    }

                    connector.ack(); // 提交确认
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        connector.unsubscribe();
        // connector.stopRunning();
    }
}

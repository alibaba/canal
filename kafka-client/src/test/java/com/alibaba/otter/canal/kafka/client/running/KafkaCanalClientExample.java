package com.alibaba.otter.canal.kafka.client.running;

import com.alibaba.otter.canal.kafka.client.KafkaCanalConnector;
import com.alibaba.otter.canal.kafka.client.KafkaCanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

public class KafkaCanalClientExample {
    protected final static Logger logger = LoggerFactory.getLogger(KafkaCanalClientExample.class);

    private KafkaCanalConnector connector;

    private volatile boolean running = false;

    private Thread thread = null;

    private Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };

    public KafkaCanalClientExample(String servers, String topic, Integer partition, String groupId) {
        connector = KafkaCanalConnectors.newKafkaConnector(servers, topic, partition, groupId);
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }
        connector.disconnect();
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
        while (running) {
            try {
                connector.subscribe();
                while (running) {
                    try {
                        Message message = connector.getWithoutAck(); //获取message
                        long batchId = message.getId();
                        int size = message.getEntries().size();
                        if (batchId == -1 || size == 0) {
                            // try {
                            // Thread.sleep(1000);
                            // } catch (InterruptedException e) {
                            // }
                        } else {
                            // printSummary(message, batchId, size);
                            // printEntry(message.getEntries());
                            logger.info(message.toString());
                        }

                        connector.ack(); // 提交确认
                    } catch (WakeupException e) {
                        //ignore
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            } catch (WakeupException e) {
                //ignore
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
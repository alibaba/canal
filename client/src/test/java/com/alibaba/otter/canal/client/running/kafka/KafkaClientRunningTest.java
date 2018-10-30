package com.alibaba.otter.canal.client.running.kafka;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.WakeupException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Kafka consumer获取Message的测试例子
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class KafkaClientRunningTest extends AbstractKafkaTest {

    private Logger  logger  = LoggerFactory.getLogger(KafkaClientRunningTest.class);

    private boolean running = true;

    @Test
    public void testKafkaConsumer() {
        final ExecutorService executor = Executors.newFixedThreadPool(1);

        final KafkaCanalConnector connector = new KafkaCanalConnector(servers, topic, partition, groupId, null, false);

        executor.submit(new Runnable() {

            @Override
            public void run() {
                connector.connect();
                connector.subscribe();
                while (running) {
                    try {
                        List<Message> messages = connector.getList(3L, TimeUnit.SECONDS);
                        if (messages != null) {
                            System.out.println(messages);
                        }
                        connector.ack();
                    } catch (WakeupException e) {
                        // ignore
                    }
                }
                connector.unsubscribe();
                connector.disconnect();
            }
        });

        sleep(60000);
        running = false;
        executor.shutdown();
        logger.info("shutdown completed");
    }

}

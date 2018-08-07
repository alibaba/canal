package com.alibaba.otter.canal.kafka.client.running;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.WakeupException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.kafka.client.KafkaCanalConnector;
import com.alibaba.otter.canal.kafka.client.KafkaCanalConnectors;
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

        final KafkaCanalConnector connector = KafkaCanalConnectors.newKafkaConnector(servers, topic, partition, groupId);

        executor.submit(new Runnable() {

            @Override
            public void run() {
                connector.connect();
                connector.subscribe();
                while (running) {
                    try {
                        Message message = connector.getWithoutAck(3L, TimeUnit.SECONDS);
                        if (message != null) {
                            System.out.println(message);
                        }
                        connector.ack();
                    } catch (WakeupException e) {
                        // ignore
                    }
                }
                connector.unsubscribe();
                connector.disconnnect();
            }
        });

        sleep(60000);
        running = false;
        executor.shutdown();
        logger.info("shutdown completed");
    }

}

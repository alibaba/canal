package com.alibaba.otter.canal.kafka.client.running;

import com.alibaba.otter.canal.kafka.client.KafkaCanalConnector;
import com.alibaba.otter.canal.kafka.client.KafkaCanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientRunningTest extends AbstractKafkaTest {
    private Logger logger = LoggerFactory.getLogger(ClientRunningTest.class);

    private boolean running = true;

    @Test
    public void testKafkaConsumer() {
        final ExecutorService executor = Executors.newFixedThreadPool(1);

        final KafkaCanalConnector connector = KafkaCanalConnectors.newKafkaConnector(servers, topic, partition, groupId);
        connector.subscribe();

        executor.submit(new Runnable() {
            @Override
            public void run() {
                while (running) {
                    try {
                        Message message = connector.getWithoutAck(3L, TimeUnit.SECONDS);
                        if (message != null) {
                            System.out.println(message);
                        }
                        connector.ack();
                    } catch (WakeupException e) {
                        //ignore
                    }
                }
            }
        });

        sleep(60000);
        connector.disconnect();
        running = false;
        executor.shutdown();
        logger.info("shutdown completed");

        sleep(10000);
    }

}

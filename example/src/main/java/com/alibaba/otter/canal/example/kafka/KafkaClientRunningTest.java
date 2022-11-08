package com.alibaba.otter.canal.example.kafka;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    public void testKafkaConsumer() {
        final ExecutorService executor = Executors.newFixedThreadPool(1);
        final KafkaCanalConnector connector = new KafkaCanalConnector(servers, topic, partition, groupId, null, false);
        executor.submit(() -> {
            connector.connect();
            connector.subscribe();
            while (running) {
                List<Message> messages = connector.getList(3L, TimeUnit.SECONDS);
                if (messages != null) {
                    System.out.println(messages);
                }
                connector.ack();
            }
            connector.unsubscribe();
            connector.disconnect();
        });

        sleep(60000);
        running = false;
        executor.shutdown();
        logger.info("shutdown completed");
    }

}

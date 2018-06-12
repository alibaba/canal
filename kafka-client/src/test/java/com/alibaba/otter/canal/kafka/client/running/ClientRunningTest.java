package com.alibaba.otter.canal.kafka.client.running;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.kafka.client.KafkaCanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientRunningTest extends AbstractKafkaTest {

    private boolean running = true;

    @Test
    public void testKafkaConsumer() {
        final ExecutorService executor = Executors.newFixedThreadPool(1);

        final CanalConnector kafkaCanalConnector = KafkaCanalConnectors.newKafkaConnector(servers, topic, partition, groupId);
        kafkaCanalConnector.connect();
        kafkaCanalConnector.subscribe();

        executor.submit(new Runnable() {
            @Override
            public void run() {
                while (running) {
                    Message message = kafkaCanalConnector.getWithoutAck(1);
                    if (message != null) {
                        System.out.println(message);
                        sleep(40000);
                    }
                    kafkaCanalConnector.ack(1);
                }
            }
        });

        sleep(120000);
        running = false;
        kafkaCanalConnector.disconnect();
    }

}

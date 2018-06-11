package com.alibaba.otter.canal.kafka.producer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.otter.canal.kafka.producer.KafkaProperties.Topic;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalKafkaStarter {
    private static final Logger logger = LoggerFactory.getLogger(CanalKafkaStarter.class);

    private volatile static boolean running = false;
    private static short CLIENT_ID = 1001;

    public static void init(KafkaProperties kafkaProperties) {
        //初始化 kafka producer
        CanalKafkaProducer.init(kafkaProperties);

        //对应每个instance启动一个worker线程
        List<Topic> topics = kafkaProperties.getTopics();

        ExecutorService executorService = Executors.newFixedThreadPool(topics.size());

        for (final Topic topic : topics) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    worker(topic);
                }
            });
        }
        running = true;
    }

    private static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            //ignore
        }
    }

    public static void worker(Topic topic) {
        while (!running) ;
        //TODO 等待canal  server启动完毕
        logger.info("## start the canal consumer: {}.", topic.getDestination());
        CanalServerWithEmbedded server = CanalServerWithEmbedded.instance();
        ClientIdentity clientIdentity = new ClientIdentity(topic.getDestination(), CLIENT_ID, "");
        while (running) {
            try {
                if (!server.getCanalInstances().containsKey(clientIdentity.getDestination())) {
                    sleep(3000);
                    continue;
                }
                server.subscribe(clientIdentity);
                logger.info("## the canal consumer {} is running now ......", topic.getDestination());

                while (running) {
                    Message message = server.getWithoutAck(clientIdentity, 5 * 1024); // 获取指定数量的数据
                    long batchId = message.getId();
                    try {
                        int size = message.getEntries().size();
                        if (batchId == -1 || size == 0) {
                            sleep(1000);
                        } else {
                            CanalKafkaProducer.send(topic, message);
                        }

                        if (batchId != -1) {
                            server.ack(clientIdentity, batchId); // 提交确认
                        }
                    } catch (Exception e) {
                        server.rollback(clientIdentity);
                        logger.error(e.getMessage(), e);
                        sleep(1000);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            }
        }
    }
}

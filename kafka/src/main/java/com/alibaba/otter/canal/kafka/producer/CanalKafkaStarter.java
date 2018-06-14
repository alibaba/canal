package com.alibaba.otter.canal.kafka.producer;

import java.io.FileInputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.kafka.CanalServerStarter;
import com.alibaba.otter.canal.kafka.producer.KafkaProperties.CanalDestination;
import com.alibaba.otter.canal.kafka.producer.KafkaProperties.Topic;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

/**
 * kafka 启动类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaStarter {

    private static final String       CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger       logger               = LoggerFactory.getLogger(CanalKafkaStarter.class);

    private volatile static boolean   running              = false;

    private static ExecutorService    executorService;

    private static CanalKafkaProducer canalKafkaProducer;

    private static KafkaProperties    kafkaProperties;

    public static void init() {
        try {
            logger.info("## load kafka configurations");
            String conf = System.getProperty("kafka.conf", "classpath:kafka.yml");

            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                kafkaProperties = new Yaml().loadAs(CanalKafkaStarter.class.getClassLoader().getResourceAsStream(conf),
                    KafkaProperties.class);
            } else {
                kafkaProperties = new Yaml().loadAs(new FileInputStream(conf), KafkaProperties.class);
            }

            // 初始化 kafka producer
            canalKafkaProducer = new CanalKafkaProducer();
            canalKafkaProducer.init(kafkaProperties);

            // 对应每个instance启动一个worker线程
            List<CanalDestination> destinations = kafkaProperties.getCanalDestinations();

            executorService = Executors.newFixedThreadPool(destinations.size());

            logger.info("## start the kafka workers.");
            for (final CanalDestination destination : destinations) {
                executorService.execute(new Runnable() {

                    @Override
                    public void run() {
                        worker(destination);
                    }
                });
            }
            running = true;
            logger.info("## the kafka workers is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the kafka workers");
                        running = false;
                        executorService.shutdown();
                        canalKafkaProducer.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping kafka workers:", e);
                    } finally {
                        logger.info("## canal kafka is down.");
                    }
                }

            });

        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal kafka workers:", e);
            System.exit(0);
        }
    }

    private static void worker(CanalDestination destination) {
        while (!running)
            ;
        while (!CanalServerStarter.isRunning())
            ; // 等待server启动完成
        logger.info("## start the canal consumer: {}.", destination.getCanalDestination());
        CanalServerWithEmbedded server = CanalServerWithEmbedded.instance();
        ClientIdentity clientIdentity = new ClientIdentity(destination.getCanalDestination(), (short) 1001, "");
        while (running) {
            try {
                if (!server.getCanalInstances().containsKey(clientIdentity.getDestination())) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    continue;
                }
                server.subscribe(clientIdentity);
                logger.info("## the canal consumer {} is running now ......", destination.getCanalDestination());

                while (running) {
                    Message message = server.getWithoutAck(clientIdentity, kafkaProperties.getCanalBatchSize()); // 获取指定数量的数据
                    long batchId = message.getId();
                    try {
                        int size = message.getEntries().size();
                        if (batchId != -1 && size != 0) {
                            if (!StringUtils.isEmpty(destination.getTopic())) {
                                Topic topic = new Topic();
                                topic.setTopic(destination.getTopic());
                                topic.setPartition(destination.getPartition());
                                destination.getTopics().add(topic);
                            }
                            for (Topic topic : destination.getTopics()) {
                                canalKafkaProducer.send(topic, message); // 发送message到所有topic
                            }
                        }

                        if (batchId != -1) {
                            server.ack(clientIdentity, batchId); // 提交确认
                        }
                    } catch (Exception e) {
                        server.rollback(clientIdentity);
                        logger.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            }
        }
    }
}

package com.alibaba.otter.canal.kafka.producer;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.otter.canal.kafka.CanalServerStarter;
import com.alibaba.otter.canal.kafka.producer.KafkaProperties.Topic;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * kafka 启动类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaStarter {
    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger = LoggerFactory.getLogger(CanalKafkaStarter.class);

    private volatile static boolean running = false;

    private static ExecutorService executorService;

    public static void init() {
        try {

            logger.info("## load kafka configurations");
            String conf = System.getProperty("kafka.conf", "classpath:kafka.yml");

            KafkaProperties kafkaProperties;
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                kafkaProperties = new Yaml().loadAs(CanalKafkaStarter.class.getClassLoader().getResourceAsStream(conf), KafkaProperties.class);
            } else {
                kafkaProperties = new Yaml().loadAs(new FileInputStream(conf), KafkaProperties.class);
            }

            //初始化 kafka producer
            CanalKafkaProducer.init(kafkaProperties);

            //对应每个instance启动一个worker线程
            List<Topic> topics = kafkaProperties.getTopics();

            executorService = Executors.newFixedThreadPool(topics.size());

            logger.info("## start the kafka workers.");
            for (final Topic topic : topics) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        worker(topic);
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
                        CanalKafkaProducer.stop();
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


    private static void worker(Topic topic) {
        while (!running) ;
        while (!CanalServerStarter.isRunning()) ; //等待server启动完成
        logger.info("## start the canal consumer: {}.", topic.getDestination());
        CanalServerWithEmbedded server = CanalServerWithEmbedded.instance();
        ClientIdentity clientIdentity = new ClientIdentity(topic.getDestination(), (short) 1001, "");
        while (running) {
            try {
                if (!server.getCanalInstances().containsKey(clientIdentity.getDestination())) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        //ignore
                    }
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
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                //ignore
                            }
                        } else {
                            CanalKafkaProducer.send(topic, message);
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

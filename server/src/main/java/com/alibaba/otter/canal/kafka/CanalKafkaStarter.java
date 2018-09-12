package com.alibaba.otter.canal.kafka;

import java.io.FileInputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.kafka.KafkaProperties.CanalDestination;
import com.alibaba.otter.canal.kafka.KafkaProperties.Topic;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.CanalServerStarter;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

/**
 * kafka 启动类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaStarter implements CanalServerStarter {

    private static final Logger logger               = LoggerFactory.getLogger(CanalKafkaStarter.class);

    private static final String CLASSPATH_URL_PREFIX = "classpath:";

    private volatile boolean    running              = false;

    private ExecutorService     executorService;

    private CanalKafkaProducer  canalKafkaProducer;

    private KafkaProperties     kafkaProperties;

    public void init() {
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
            // set filterTransactionEntry
            // if (kafkaProperties.isFilterTransactionEntry()) {
            // System.setProperty("canal.instance.filter.transaction.entry", "true");
            // }
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

    private void worker(CanalDestination destination) {
        while (!running)
            ;
        logger.info("## start the canal consumer: {}.", destination.getCanalDestination());
        final CanalServerWithEmbedded server = CanalServerWithEmbedded.instance();
        final ClientIdentity clientIdentity = new ClientIdentity(destination.getCanalDestination(), (short) 1001, "");
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
                    Message message;
                    if (kafkaProperties.getCanalGetTimeout() != null) {
                        message = server.getWithoutAck(clientIdentity,
                            kafkaProperties.getCanalBatchSize(),
                            kafkaProperties.getCanalGetTimeout(),
                            TimeUnit.MILLISECONDS);
                    } else {
                        message = server.getWithoutAck(clientIdentity, kafkaProperties.getCanalBatchSize());
                    }

                    final long batchId = message.getId();
                    try {
                        int size = message.isRaw() ? message.getRawEntries().size() : message.getEntries().size();
                        if (batchId != -1 && size != 0) {
                            Topic topic = new Topic();
                            topic.setTopic(destination.getTopic());
                            topic.setPartition(destination.getPartition());
                            canalKafkaProducer.send(topic, message, new CanalKafkaProducer.Callback() {

                                @Override
                                public void commit() {
                                    server.ack(clientIdentity, batchId); // 提交确认
                                }

                                @Override
                                public void rollback() {
                                    server.rollback(clientIdentity, batchId);
                                }
                            }); // 发送message到topic
                        } else {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }

                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            }
        }
    }
}

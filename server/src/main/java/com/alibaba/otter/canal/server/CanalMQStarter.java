package com.alibaba.otter.canal.server;

import java.io.FileInputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.kafka.CanalKafkaProducer;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.spi.CanalMQProducer;

public class CanalMQStarter {

    private static final Logger logger               = LoggerFactory.getLogger(CanalMQStarter.class);

    private static final String CLASSPATH_URL_PREFIX = "classpath:";

    private volatile boolean    running              = false;

    private ExecutorService     executorService;

    private CanalMQProducer     canalMQProducer;

    private MQProperties        properties;

    public CanalMQStarter(CanalMQProducer canalMQProducer){
        this.canalMQProducer = canalMQProducer;
    }

    public void init() {
        try {
            logger.info("## load MQ configurations");
            String conf = System.getProperty("mq.conf", "classpath:mq.yml");

            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties = new Yaml().loadAs(CanalMQStarter.class.getClassLoader().getResourceAsStream(conf),
                    MQProperties.class);
            } else {
                properties = new Yaml().loadAs(new FileInputStream(conf), MQProperties.class);
            }

            // 初始化 kafka producer
            // canalMQProducer = new CanalKafkaProducer();
            canalMQProducer.init(properties);
            // set filterTransactionEntry
            if (properties.isFilterTransactionEntry()) {
                System.setProperty("canal.instance.filter.transaction.entry", "true");
            }

            // 对应每个instance启动一个worker线程
            List<MQProperties.CanalDestination> destinations = properties.getCanalDestinations();

            executorService = Executors.newFixedThreadPool(destinations.size());

            logger.info("## start the MQ workers.");
            for (final MQProperties.CanalDestination destination : destinations) {
                executorService.execute(new Runnable() {

                    @Override
                    public void run() {
                        worker(destination);
                    }
                });
            }
            running = true;
            logger.info("## the MQ workers is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the MQ workers");
                        running = false;
                        executorService.shutdown();
                        canalMQProducer.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping MQ workers:", e);
                    } finally {
                        logger.info("## canal MQ is down.");
                    }
                }

            });

        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal MQ workers:", e);
            System.exit(0);
        }
    }

    private void worker(MQProperties.CanalDestination destination) {
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
                    if (properties.getCanalGetTimeout() != null) {
                        message = server.getWithoutAck(clientIdentity,
                            properties.getCanalBatchSize(),
                            properties.getCanalGetTimeout(),
                            TimeUnit.MILLISECONDS);
                    } else {
                        message = server.getWithoutAck(clientIdentity, properties.getCanalBatchSize());
                    }

                    final long batchId = message.getId();
                    try {
                        int size = message.isRaw() ? message.getRawEntries().size() : message.getEntries().size();
                        if (batchId != -1 && size != 0) {
                            canalMQProducer.send(destination, message, new CanalKafkaProducer.Callback() {

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

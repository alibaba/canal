package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.kafka.CanalKafkaProducer;
import com.alibaba.otter.canal.rocketmq.CanalRocketMQProducer;
import com.alibaba.otter.canal.server.CanalMQStarter;
import com.alibaba.otter.canal.spi.CanalMQProducer;

/**
 * canal独立版本启动的入口类
 *
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */
public class CanalLauncher {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(CanalLauncher.class);

    public static void main(String[] args) throws Throwable {
        try {
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();

            logger.info("## load canal configurations");
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }

            CanalMQProducer canalMQProducer = null;
            String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
            if (serverMode.equalsIgnoreCase("kafka")) {
                canalMQProducer = new CanalKafkaProducer();
            } else if (serverMode.equalsIgnoreCase("rocketmq")) {
                canalMQProducer = new CanalRocketMQProducer();
            }

            if (canalMQProducer != null) {
                // disable netty
                System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            }

            logger.info("## start the canal server.");
            final CanalController controller = new CanalController(properties);
            controller.start();
            logger.info("## the canal server is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the canal server");
                        controller.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping canal Server:", e);
                    } finally {
                        logger.info("## canal server is down.");
                    }
                }

            });

            if (canalMQProducer != null) {
                CanalMQStarter canalMQStarter = new CanalMQStarter(canalMQProducer);
                MQProperties mqProperties = buildMQPosition(properties);
                canalMQStarter.start(mqProperties);
                controller.setCanalMQStarter(canalMQStarter);
            }
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
            System.exit(0);
        }
    }

    private static MQProperties buildMQPosition(Properties properties) {
        MQProperties mqProperties = new MQProperties();
        mqProperties.setServers(CanalController.getProperty(properties, CanalConstants.CANAL_MQ_SERVERS));
        mqProperties.setRetries(Integer.valueOf(CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_RETRIES)));
        mqProperties.setBatchSize(Integer.valueOf(CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_BATCHSIZE)));
        mqProperties.setLingerMs(Integer.valueOf(CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_LINGERMS)));
        mqProperties.setBufferMemory(Long.valueOf(CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_BUFFERMEMORY)));
        mqProperties.setCanalBatchSize(Integer.valueOf(CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_CANALBATCHSIZE)));
        mqProperties.setCanalGetTimeout(Long.valueOf(CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_CANALGETTIMEOUT)));
        mqProperties.setFlatMessage(Boolean.valueOf(CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_FLATMESSAGE)));
        mqProperties.setCompressionType(CanalController.getProperty(properties,CanalConstants.CANAL_MQ_COMPRESSION_TYPE));
        mqProperties.setAcks(CanalController.getProperty(properties,CanalConstants.CANAL_MQ_ACKS));
        return mqProperties;
    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("UnCaughtException", e);
            }
        });
    }
}

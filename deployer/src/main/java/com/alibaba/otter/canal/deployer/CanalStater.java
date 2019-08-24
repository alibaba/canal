package com.alibaba.otter.canal.deployer;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.kafka.CanalKafkaProducer;
import com.alibaba.otter.canal.rocketmq.CanalRocketMQProducer;
import com.alibaba.otter.canal.server.CanalMQStarter;
import com.alibaba.otter.canal.spi.CanalMQProducer;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Canal server 启动类
 *
 * @author rewerma 2018-12-30 下午05:12:16
 * @version 1.0.1
 */
public class CanalStater {

    private static final Logger logger          = LoggerFactory.getLogger(CanalStater.class);

    private CanalController     controller      = null;
    private CanalMQProducer     canalMQProducer = null;
    private Thread              shutdownThread  = null;
    private CanalMQStarter      canalMQStarter  = null;
    private volatile Properties properties;
    private volatile boolean    running         = false;

    public CanalStater(Properties properties){
        this.properties = properties;
    }

    public boolean isRunning() {
        return running;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public CanalController getController() {
        return controller;
    }

    /**
     * 启动方法
     *
     * @throws Throwable
     */
    public synchronized void start() throws Throwable {
        String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
        if (serverMode.equalsIgnoreCase("kafka")) {
            canalMQProducer = new CanalKafkaProducer();
        } else if (serverMode.equalsIgnoreCase("rocketmq")) {
            canalMQProducer = new CanalRocketMQProducer();
        }

        if (canalMQProducer != null) {
            // disable netty
            System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            String autoScan = CanalController.getProperty(properties, CanalConstants.CANAL_AUTO_SCAN);
            // 设置为raw避免ByteString->Entry的二次解析
            System.setProperty("canal.instance.memory.rawEntry", "false");
            if ("true".equals(autoScan)) {
                String rootDir = CanalController.getProperty(properties, CanalConstants.CANAL_CONF_DIR);
                if (StringUtils.isEmpty(rootDir)) {
                    rootDir = "../conf";
                }
                File rootdir = new File(rootDir);
                if (rootdir.exists()) {
                    File[] instanceDirs = rootdir.listFiles(new FileFilter() {

                        public boolean accept(File pathname) {
                            String filename = pathname.getName();
                            return pathname.isDirectory() && !"spring".equalsIgnoreCase(filename)
                                   && !"metrics".equalsIgnoreCase(filename);
                        }
                    });
                    if (instanceDirs != null && instanceDirs.length > 0) {
                        List<String> instances = Lists.transform(Arrays.asList(instanceDirs),
                            new Function<File, String>() {

                                @Override
                                public String apply(File instanceDir) {
                                    return instanceDir.getName();
                                }
                            });
                        System.setProperty(CanalConstants.CANAL_DESTINATIONS, Joiner.on(",").join(instances));
                    }
                }
            } else {
                String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
                System.setProperty(CanalConstants.CANAL_DESTINATIONS, destinations);
            }
        }

        logger.info("## start the canal server.");
        controller = new CanalController(properties);
        controller.start();
        logger.info("## the canal server is running now ......");
        shutdownThread = new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal server");
                    controller.stop();
                    CanalLauncher.runningLatch.countDown();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal Server:", e);
                } finally {
                    logger.info("## canal server is down.");
                }
            }

        };
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        if (canalMQProducer != null) {
            canalMQStarter = new CanalMQStarter(canalMQProducer);
            MQProperties mqProperties = buildMQProperties(properties);
            canalMQStarter.start(mqProperties);
            controller.setCanalMQStarter(canalMQStarter);
        }

        running = true;
    }

    /**
     * 销毁方法，远程配置变更时调用
     *
     * @throws Throwable
     */
    public synchronized void stop() throws Throwable {
        if (controller != null) {
            controller.stop();
            controller = null;
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
        if (canalMQProducer != null && canalMQStarter != null) {
            canalMQStarter.destroy();
            canalMQStarter = null;
            canalMQProducer = null;
        }
        running = false;
    }

    /**
     * 构造MQ对应的配置
     *
     * @param properties canal.properties 配置
     * @return
     */
    private static MQProperties buildMQProperties(Properties properties) {
        MQProperties mqProperties = new MQProperties();
        String servers = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_SERVERS);
        if (!StringUtils.isEmpty(servers)) {
            mqProperties.setServers(servers);
        }
        String retires = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_RETRIES);
        if (!StringUtils.isEmpty(retires)) {
            mqProperties.setRetries(Integer.valueOf(retires));
        }
        String batchSize = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_BATCHSIZE);
        if (!StringUtils.isEmpty(batchSize)) {
            mqProperties.setBatchSize(Integer.valueOf(batchSize));
        }
        String lingerMs = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_LINGERMS);
        if (!StringUtils.isEmpty(lingerMs)) {
            mqProperties.setLingerMs(Integer.valueOf(lingerMs));
        }
        String maxRequestSize = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_MAXREQUESTSIZE);
        if (!StringUtils.isEmpty(maxRequestSize)) {
            mqProperties.setMaxRequestSize(Integer.valueOf(maxRequestSize));
        }
        String bufferMemory = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_BUFFERMEMORY);
        if (!StringUtils.isEmpty(bufferMemory)) {
            mqProperties.setBufferMemory(Long.valueOf(bufferMemory));
        }
        String canalBatchSize = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_CANALBATCHSIZE);
        if (!StringUtils.isEmpty(canalBatchSize)) {
            mqProperties.setCanalBatchSize(Integer.valueOf(canalBatchSize));
        }
        String canalGetTimeout = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_CANALGETTIMEOUT);
        if (!StringUtils.isEmpty(canalGetTimeout)) {
            mqProperties.setCanalGetTimeout(Long.valueOf(canalGetTimeout));
        }
        String flatMessage = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_FLATMESSAGE);
        if (!StringUtils.isEmpty(flatMessage)) {
            mqProperties.setFlatMessage(Boolean.valueOf(flatMessage));
        }
        String compressionType = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_COMPRESSION_TYPE);
        if (!StringUtils.isEmpty(compressionType)) {
            mqProperties.setCompressionType(compressionType);
        }
        String acks = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_ACKS);
        if (!StringUtils.isEmpty(acks)) {
            mqProperties.setAcks(acks);
        }
        String aliyunAccessKey = CanalController.getProperty(properties, CanalConstants.CANAL_ALIYUN_ACCESSKEY);
        if (!StringUtils.isEmpty(aliyunAccessKey)) {
            mqProperties.setAliyunAccessKey(aliyunAccessKey);
        }
        String aliyunSecretKey = CanalController.getProperty(properties, CanalConstants.CANAL_ALIYUN_SECRETKEY);
        if (!StringUtils.isEmpty(aliyunSecretKey)) {
            mqProperties.setAliyunSecretKey(aliyunSecretKey);
        }

        String producerGroup = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_PRODUCERGROUP);
        if (!StringUtils.isEmpty(producerGroup)) {
            mqProperties.setProducerGroup(producerGroup);
        }

        String enableMessageTrace = CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_ENABLE_MESSAGE_TRACE);
        if (!StringUtils.isEmpty(enableMessageTrace)) {
            mqProperties.setEnableMessageTrace(Boolean.valueOf(enableMessageTrace));
        }

        String accessChannel = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_ACCESS_CHANNEL);
        if (!StringUtils.isEmpty(accessChannel)) {
            mqProperties.setAccessChannel(accessChannel);
        }

        String customizedTraceTopic = CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_CUSTOMIZED_TRACE_TOPIC);
        if (!StringUtils.isEmpty(customizedTraceTopic)) {
            mqProperties.setCustomizedTraceTopic(customizedTraceTopic);
        }

        String namespace = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_NAMESPACE);
        if (!StringUtils.isEmpty(namespace)) {
            mqProperties.setNamespace(namespace);
        }

        String kafkaKerberosEnable = CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_KAFKA_KERBEROS_ENABLE);
        if (!StringUtils.isEmpty(kafkaKerberosEnable)) {
            mqProperties.setKerberosEnable(Boolean.valueOf(kafkaKerberosEnable));
        }

        String kafkaKerberosKrb5Filepath = CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_KAFKA_KERBEROS_KRB5FILEPATH);
        if (!StringUtils.isEmpty(kafkaKerberosKrb5Filepath)) {
            mqProperties.setKerberosKrb5FilePath(kafkaKerberosKrb5Filepath);
        }

        String kafkaKerberosJaasFilepath = CanalController.getProperty(properties,
            CanalConstants.CANAL_MQ_KAFKA_KERBEROS_JAASFILEPATH);
        if (!StringUtils.isEmpty(kafkaKerberosJaasFilepath)) {
            mqProperties.setKerberosJaasFilePath(kafkaKerberosJaasFilepath);
        }

        for (Object key : properties.keySet()) {
            key = StringUtils.trim(key.toString());
            if (((String) key).startsWith(CanalConstants.CANAL_MQ_PROPERTIES)) {
                String value = CanalController.getProperty(properties, (String) key);
                String subKey = ((String) key).substring(CanalConstants.CANAL_MQ_PROPERTIES.length() + 1);
                mqProperties.getProperties().put(subKey, value);
            }
        }

        return mqProperties;
    }
}

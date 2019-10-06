package com.alibaba.otter.canal.deployer;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.netty.CanalAdminWithNetty;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.deployer.admin.CanalAdminController;
import com.alibaba.otter.canal.kafka.CanalKafkaProducer;
import com.alibaba.otter.canal.rabbitmq.CanalRabbitMQProducer;
import com.alibaba.otter.canal.rocketmq.CanalRocketMQProducer;
import com.alibaba.otter.canal.server.CanalMQStarter;
import com.alibaba.otter.canal.spi.CanalMQProducer;

/**
 * Canal server 启动类
 *
 * @author rewerma 2018-12-30 下午05:12:16
 * @version 1.0.1
 */
public class CanalStarter {

    private static final Logger logger          = LoggerFactory.getLogger(CanalStarter.class);

    private CanalController     controller      = null;
    private CanalMQProducer     canalMQProducer = null;
    private Thread              shutdownThread  = null;
    private CanalMQStarter      canalMQStarter  = null;
    private volatile Properties properties;
    private volatile boolean    running         = false;

    private CanalAdminWithNetty canalAdmin;

    public CanalStarter(Properties properties){
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
        } else if (serverMode.equalsIgnoreCase("rabbitmq")) {
            canalMQProducer = new CanalRabbitMQProducer();
        }

        MQProperties mqProperties = null;
        if (canalMQProducer != null) {
            mqProperties = buildMQProperties(properties);
            // disable netty
            System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            if (mqProperties.getFlatMessage()) {
                // 设置为raw避免ByteString->Entry的二次解析
                System.setProperty("canal.instance.memory.rawEntry", "false");
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
            String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
            canalMQStarter.start(mqProperties, destinations);
            controller.setCanalMQStarter(canalMQStarter);
        }

        // start canalAdmin
        String port = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT);
        if (canalAdmin == null && StringUtils.isNotEmpty(port)) {
            String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
            String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
            CanalAdminController canalAdmin = new CanalAdminController(this);
            canalAdmin.setUser(user);
            canalAdmin.setPasswd(passwd);

            String ip = CanalController.getProperty(properties, CanalConstants.CANAL_IP);

            logger.debug("canal admin port:{}, canal admin user:{}, canal admin password: {}, canal ip:{}", port, user, passwd, ip);

            CanalAdminWithNetty canalAdminWithNetty = CanalAdminWithNetty.instance();
            canalAdminWithNetty.setCanalAdmin(canalAdmin);
            canalAdminWithNetty.setPort(Integer.valueOf(port));
            canalAdminWithNetty.setIp(ip);
            canalAdminWithNetty.start();
            this.canalAdmin = canalAdminWithNetty;
        }

        running = true;
    }

    public synchronized void stop() throws Throwable {
        stop(false);
    }

    /**
     * 销毁方法，远程配置变更时调用
     *
     * @throws Throwable
     */
    public synchronized void stop(boolean stopByAdmin) throws Throwable {
        if (!stopByAdmin && canalAdmin != null) {
            canalAdmin.stop();
            canalAdmin = null;
        }

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
        String parallelThreadSize = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_PARALLELTHREADSIZE);
        if (!StringUtils.isEmpty(parallelThreadSize)) {
            mqProperties.setParallelThreadSize(Integer.valueOf(parallelThreadSize));
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

        String vhost = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_VHOST);
        if (!StringUtils.isEmpty(vhost)) {
            mqProperties.setVhost(vhost);
        }

        String username = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_USERNAME);
        if (!StringUtils.isEmpty(username)) {
            mqProperties.setUsername(username);
        }

        String password = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_PASSWORD);
        if (!StringUtils.isEmpty(password)) {
            mqProperties.setPassword(password);
        }

        String aliyunUID = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_ALIYUN_UID);
        if (!StringUtils.isEmpty(aliyunUID)) {
            mqProperties.setAliyunUID(Long.valueOf(aliyunUID));
        }

        String exchange = CanalController.getProperty(properties, CanalConstants.CANAL_MQ_EXCHANGE);
        if (!StringUtils.isEmpty(exchange)) {
            mqProperties.setExchange(exchange);
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

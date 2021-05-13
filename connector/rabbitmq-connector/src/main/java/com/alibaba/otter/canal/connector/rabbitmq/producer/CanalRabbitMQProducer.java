package com.alibaba.otter.canal.connector.rabbitmq.producer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.common.utils.PropertiesUtils;
import com.alibaba.otter.canal.connector.core.producer.AbstractMQProducer;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.producer.MQMessageUtils;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.connector.rabbitmq.config.RabbitMQConstants;
import com.alibaba.otter.canal.connector.rabbitmq.config.RabbitMQProducerConfig;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * RabbitMQ Producer SPI 实现
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
@SPI("rabbitmq")
public class CanalRabbitMQProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalRabbitMQProducer.class);

    private Connection          connect;
    private Channel             channel;

    @Override
    public void init(Properties properties) {
        RabbitMQProducerConfig rabbitMQProperties = new RabbitMQProducerConfig();
        this.mqProperties = rabbitMQProperties;
        super.init(properties);
        loadRabbitMQProperties(properties);

        ConnectionFactory factory = new ConnectionFactory();
        String servers = rabbitMQProperties.getHost();
        if (servers.contains(":")) {
            String[] serverHostAndPort = servers.split(":");
            factory.setHost(serverHostAndPort[0]);
            factory.setPort(Integer.parseInt(serverHostAndPort[1]));
        } else {
            factory.setHost(servers);
        }

        if (mqProperties.getAliyunAccessKey().length() > 0 && mqProperties.getAliyunSecretKey().length() > 0
            && mqProperties.getAliyunUid() > 0) {
            factory.setCredentialsProvider(new AliyunCredentialsProvider(mqProperties.getAliyunAccessKey(),
                mqProperties.getAliyunSecretKey(),
                mqProperties.getAliyunUid()));
        } else {
            factory.setUsername(rabbitMQProperties.getUsername());
            factory.setPassword(rabbitMQProperties.getPassword());
        }
        factory.setVirtualHost(rabbitMQProperties.getVirtualHost());
        try {
            connect = factory.newConnection();
            channel = connect.createChannel();
            // channel.exchangeDeclare(mqProperties.getExchange(), "topic");
        } catch (IOException | TimeoutException ex) {
            throw new CanalException("Start RabbitMQ producer error", ex);
        }
    }

    private void loadRabbitMQProperties(Properties properties) {
        RabbitMQProducerConfig rabbitMQProperties = (RabbitMQProducerConfig) this.mqProperties;
        // 兼容下<=1.1.4的mq配置
        doMoreCompatibleConvert("canal.mq.servers", "rabbitmq.host", properties);

        String host = PropertiesUtils.getProperty(properties, RabbitMQConstants.RABBITMQ_HOST);
        if (!StringUtils.isEmpty(host)) {
            rabbitMQProperties.setHost(host);
        }
        String vhost = PropertiesUtils.getProperty(properties, RabbitMQConstants.RABBITMQ_VIRTUAL_HOST);
        if (!StringUtils.isEmpty(vhost)) {
            rabbitMQProperties.setVirtualHost(vhost);
        }
        String exchange = PropertiesUtils.getProperty(properties, RabbitMQConstants.RABBITMQ_EXCHANGE);
        if (!StringUtils.isEmpty(exchange)) {
            rabbitMQProperties.setExchange(exchange);
        }
        String username = PropertiesUtils.getProperty(properties, RabbitMQConstants.RABBITMQ_USERNAME);
        if (!StringUtils.isEmpty(username)) {
            rabbitMQProperties.setUsername(username);
        }
        String password = PropertiesUtils.getProperty(properties, RabbitMQConstants.RABBITMQ_PASSWORD);
        if (!StringUtils.isEmpty(password)) {
            rabbitMQProperties.setPassword(password);
        }
    }

    @Override
    public void send(final MQDestination destination, Message message, Callback callback) {
        ExecutorTemplate template = new ExecutorTemplate(sendExecutor);
        try {
            if (!StringUtils.isEmpty(destination.getDynamicTopic())) {
                // 动态topic
                Map<String, Message> messageMap = MQMessageUtils.messageTopics(message,
                    destination.getTopic(),
                    destination.getDynamicTopic());

                for (Map.Entry<String, com.alibaba.otter.canal.protocol.Message> entry : messageMap.entrySet()) {
                    final String topicName = entry.getKey().replace('.', '_');
                    final com.alibaba.otter.canal.protocol.Message messageSub = entry.getValue();

                    template.submit(() -> send(destination, topicName, messageSub));
                }

                template.waitForResult();
            } else {
                send(destination, destination.getTopic(), message);
            }
            callback.commit();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            callback.rollback();
        } finally {
            template.clear();
        }
    }

    private void send(MQDestination canalDestination, String topicName, Message messageSub) {
        if (!mqProperties.isFlatMessage()) {
            byte[] message = CanalMessageSerializerUtil.serializer(messageSub, mqProperties.isFilterTransactionEntry());
            if (logger.isDebugEnabled()) {
                logger.debug("send message:{} to destination:{}", message, canalDestination.getCanalDestination());
            }
            sendMessage(topicName, message);
        } else {
            // 并发构造
            MQMessageUtils.EntryRowData[] datas = MQMessageUtils.buildMessageData(messageSub, buildExecutor);
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, messageSub.getId());
            for (FlatMessage flatMessage : flatMessages) {
                byte[] message = JSON.toJSONBytes(flatMessage, SerializerFeature.WriteMapNullValue);
                if (logger.isDebugEnabled()) {
                    logger.debug("send message:{} to destination:{}", message, canalDestination.getCanalDestination());
                }
                sendMessage(topicName, message);
            }
        }

    }

    private void sendMessage(String queueName, byte[] message) {
        // tips: 目前逻辑中暂不处理对exchange处理，请在Console后台绑定 才可使用routekey
        try {
            RabbitMQProducerConfig rabbitMQProperties = (RabbitMQProducerConfig) this.mqProperties;
            channel.basicPublish(rabbitMQProperties.getExchange(), queueName, null, message);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop RabbitMQ producer##");
        try {
            this.connect.close();
            this.channel.close();
            super.stop();
        } catch (AlreadyClosedException ex) {
            logger.error("Connection is already closed", ex);
        } catch (IOException | TimeoutException ex) {
            throw new CanalException("Stop RabbitMQ producer error", ex);
        }

        super.stop();
    }
}

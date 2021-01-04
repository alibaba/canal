package com.alibaba.otter.canal.connector.rabbitmq.consumer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.connector.core.config.CanalConstants;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.spi.CanalMsgConsumer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.connector.core.util.MessageUtil;
import com.alibaba.otter.canal.connector.rabbitmq.config.RabbitMQConstants;
import com.alibaba.otter.canal.connector.rabbitmq.producer.AliyunCredentialsProvider;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * RabbitMQ consumer SPI 实现
 *
 * @author rewerma 2020-02-01
 * @version 1.0.0
 */
@SPI("rabbitmq")
public class CanalRabbitMQConsumer implements CanalMsgConsumer {

    private static final Logger                                logger              = LoggerFactory.getLogger(CanalRabbitMQConsumer.class);

    // 链接地址
    private String                                             nameServer;
    // 主机名
    private String                                             vhost;
    private String                                             queueName;

    // 一些鉴权信息
    private String                                             accessKey;
    private String                                             secretKey;
    private Long                                               resourceOwnerId;
    private String                                             username;
    private String                                             password;

    private boolean                                            flatMessage;

    private Connection                                         connect;
    private Channel                                            channel;

    private long                                               batchProcessTimeout = 60 * 1000;
    private BlockingQueue<ConsumerBatchMessage<CommonMessage>> messageBlockingQueue;
    private volatile ConsumerBatchMessage<CommonMessage>       lastGetBatchMessage = null;

    @Override
    public void init(Properties properties, String topic, String groupId) {
        this.nameServer = properties.getProperty("rabbitmq.host");
        this.vhost = properties.getProperty("rabbitmq.virtual.host");
        this.queueName = topic;
        this.accessKey = properties.getProperty(CanalConstants.CANAL_ALIYUN_ACCESS_KEY);
        this.secretKey = properties.getProperty(CanalConstants.CANAL_ALIYUN_SECRET_KEY);
        this.username = properties.getProperty(RabbitMQConstants.RABBITMQ_USERNAME);
        this.password = properties.getProperty(RabbitMQConstants.RABBITMQ_PASSWORD);
        Long resourceOwnerIdPro = (Long) properties.get(RabbitMQConstants.RABBITMQ_RESOURCE_OWNERID);
        if (resourceOwnerIdPro != null) {
            this.resourceOwnerId = resourceOwnerIdPro;
        }
        this.flatMessage = (Boolean) properties.get(CanalConstants.CANAL_MQ_FLAT_MESSAGE);
        this.messageBlockingQueue = new LinkedBlockingQueue<>(1024);
    }

    @Override
    public void connect() {
        ConnectionFactory factory = new ConnectionFactory();
        if (accessKey.length() > 0 && secretKey.length() > 0) {
            factory.setCredentialsProvider(new AliyunCredentialsProvider(accessKey, secretKey, resourceOwnerId));
        } else {
            factory.setUsername(username);
            factory.setPassword(password);
        }
        factory.setHost(nameServer);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);
        factory.setVirtualHost(vhost);
        try {
            connect = factory.newConnection();
            channel = connect.createChannel();
        } catch (IOException | TimeoutException e) {
            throw new CanalClientException("Start RabbitMQ producer error", e);
        }

        // 不存在连接 则重新连接
        if (connect == null) {
            this.connect();
        }

        Consumer consumer = new DefaultConsumer(channel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {

                if (body != null) {
                    channel.basicAck(envelope.getDeliveryTag(), process(body));
                }
            }
        };
        try {
            channel.basicConsume(queueName, false, consumer);
        } catch (IOException e) {
            throw new CanalClientException("error", e);
        }
    }

    private boolean process(byte[] messageData) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get Message: {}", new String(messageData));
        }
        List<CommonMessage> messageList = Lists.newArrayList();
        if (!flatMessage) {
            Message message = CanalMessageSerializerUtil.deserializer(messageData);
            messageList.addAll(MessageUtil.convert(message));
        } else {
            CommonMessage commonMessage = JSON.parseObject(messageData, CommonMessage.class);
            messageList.add(commonMessage);
        }
        ConsumerBatchMessage<CommonMessage> batchMessage = new ConsumerBatchMessage<>(messageList);
        try {
            messageBlockingQueue.put(batchMessage);
        } catch (InterruptedException e) {
            logger.error("Put message to queue error", e);
            throw new RuntimeException(e);
        }
        boolean isCompleted;
        try {
            isCompleted = batchMessage.waitFinish(batchProcessTimeout);
        } catch (InterruptedException e) {
            logger.error("Interrupted when waiting messages to be finished.", e);
            throw new RuntimeException(e);
        }
        boolean isSuccess = batchMessage.isSuccess();
        return isCompleted && isSuccess;
    }

    @Override
    public List<CommonMessage> getMessage(Long timeout, TimeUnit unit) {
        try {
            if (this.lastGetBatchMessage != null) {
                throw new CanalClientException("mq get/ack not support concurrent & async ack");
            }

            ConsumerBatchMessage<CommonMessage> batchMessage = messageBlockingQueue.poll(timeout, unit);
            if (batchMessage != null) {
                this.lastGetBatchMessage = batchMessage;
                return batchMessage.getData();
            }
        } catch (InterruptedException ex) {
            logger.warn("Get message timeout", ex);
            throw new CanalClientException("Failed to fetch the data after: " + timeout);
        }
        return null;
    }

    @Override
    public void rollback() {
        try {
            if (this.lastGetBatchMessage != null) {
                this.lastGetBatchMessage.fail();
            }
        } finally {
            this.lastGetBatchMessage = null;
        }
    }

    @Override
    public void ack() {
        try {
            if (this.lastGetBatchMessage != null) {
                this.lastGetBatchMessage.ack();
            }
        } catch (Throwable e) {
            if (this.lastGetBatchMessage != null) {
                this.lastGetBatchMessage.fail();
            }
        } finally {
            this.lastGetBatchMessage = null;
        }
    }

    @Override
    public void disconnect() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException | TimeoutException e) {
                throw new CanalClientException("stop channel error", e);
            }
        }

        if (connect != null) {
            try {
                connect.close();
            } catch (IOException e) {
                throw new CanalClientException("stop connect error", e);
            }
        }
    }
}

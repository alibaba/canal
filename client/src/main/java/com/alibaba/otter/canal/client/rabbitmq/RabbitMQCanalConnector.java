package com.alibaba.otter.canal.client.rabbitmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.client.ConsumerBatchMessage;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitMQCanalConnector implements CanalMQConnector {

    private static final Logger                 logger              = LoggerFactory
        .getLogger(RabbitMQCanalConnector.class);

    // 链接地址
    private String                              nameServer;

    // 主机名
    private String                              vhost;

    private String                              queueName;

    // 一些鉴权信息
    private String                              accessKey;
    private String                              secretKey;
    private Long                                resourceOwnerId;
    private String                              username;
    private String                              password;

    private boolean                             flatMessage;

    private Connection                          connect;
    private Channel                             channel;


    private long                                batchProcessTimeout = 60 * 1000;
    private BlockingQueue<ConsumerBatchMessage> messageBlockingQueue;
    private volatile ConsumerBatchMessage       lastGetBatchMessage = null;

    public RabbitMQCanalConnector(String nameServer, String vhost, String queueName, String accessKey, String secretKey,
                                  String username, String password, Long resourceOwnerId, boolean flatMessage){
        this.nameServer = nameServer;
        this.vhost = vhost;
        this.queueName = queueName;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.username = username;
        this.password = password;
        this.resourceOwnerId = resourceOwnerId;
        this.flatMessage = flatMessage;
        this.messageBlockingQueue = new LinkedBlockingQueue<>(1024);
    }

    public void connect() throws CanalClientException {
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
    }

    @Override
    public void disconnect() throws CanalClientException {
        if (connect != null) {
            try {
                connect.close();
            } catch (IOException e) {
                throw new CanalClientException("stop connect error", e);
            }
        }
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException | TimeoutException e) {
                throw new CanalClientException("stop channel error", e);
            }
        }
    }

    @Override
    public boolean checkValid() throws CanalClientException {
        return true; // 永远true
    }

    /**
     * RabbitMQ支持拉取 不需要订阅
     *
     * @param filter
     * @throws CanalClientException
     */
    @Override
    public void subscribe(String filter) throws CanalClientException {
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

    @Override
    public void subscribe() throws CanalClientException {
        this.subscribe(null);
    }

    @Override
    public void unsubscribe() throws CanalClientException {
        // 取消订阅 直接强行断开吧
        this.disconnect();
    }

    @Override
    public Message get(int batchSize) throws CanalClientException {
        return null;
    }

    @Override
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        return null;
    }

    @Override
    public Message getWithoutAck(int batchSize) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void ack(long batchId) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void rollback(long batchId) throws CanalClientException {
        throw new CanalClientException("mq not support this method");

    }

    @Override
    public List<Message> getList(Long timeout, TimeUnit unit) throws CanalClientException {
        List<Message> messages = getListWithoutAck(timeout, unit);
        if (messages != null && !messages.isEmpty()) {
            ack();
        }
        return messages;
    }

    @Override
    public List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        try {
            if (this.lastGetBatchMessage != null) {
                throw new CanalClientException("mq get/ack not support concurrent & async ack");
            }

            ConsumerBatchMessage batchMessage = messageBlockingQueue.poll(timeout, unit);
            if (batchMessage != null) {
                this.lastGetBatchMessage = batchMessage;
                return batchMessage.getData();
            }
        } catch (InterruptedException ex) {
            logger.warn("Get message timeout", ex);
            throw new CanalClientException("Failed to fetch the data after: " + timeout);
        }
        return Lists.newArrayList();
    }

    @Override
    public List<FlatMessage> getFlatList(Long timeout, TimeUnit unit) throws CanalClientException {
        List<FlatMessage> messages = getFlatListWithoutAck(timeout, unit);
        if (messages != null && !messages.isEmpty()) {
            ack();
        }
        return messages;
    }

    @Override
    public List<FlatMessage> getFlatListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        try {
            if (this.lastGetBatchMessage != null) {
                throw new CanalClientException("mq get/ack not support concurrent & async ack");
            }

            ConsumerBatchMessage batchMessage = messageBlockingQueue.poll(timeout, unit);
            if (batchMessage != null) {
                this.lastGetBatchMessage = batchMessage;
                return batchMessage.getData();
            }
        } catch (InterruptedException ex) {
            logger.warn("Get message timeout", ex);
            throw new CanalClientException("Failed to fetch the data after: " + timeout);
        }
        return Lists.newArrayList();
    }

    private boolean process(byte[] messageData) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get Message: {}", new String(messageData));
        }
        List messageList = Lists.newArrayList();
        if (!flatMessage) {
            Message message = CanalMessageDeserializer.deserializer(messageData);
            messageList.add(message);
        } else {
            FlatMessage flatMessage = JSON.parseObject(messageData, FlatMessage.class);
            messageList.add(flatMessage);
        }
        ConsumerBatchMessage batchMessage;
        if (!flatMessage) {
            batchMessage = new ConsumerBatchMessage<Message>(messageList);
        } else {
            batchMessage = new ConsumerBatchMessage<FlatMessage>(messageList);
        }
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
    public void ack() throws CanalClientException {
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
    public void rollback() throws CanalClientException {
        try {
            if (this.lastGetBatchMessage != null) {
                this.lastGetBatchMessage.fail();
            }
        } finally {
            this.lastGetBatchMessage = null;
        }
    }

}

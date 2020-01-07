package com.alibaba.otter.canal.client.rocketmq;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.client.ConsumerBatchMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;

/**
 * RocketMQ的连接
 * 
 * <pre>
 * 注意点:
 * 1. 相比于canal {@linkplain SimpleCanalConnector}, 这里get和ack操作不能有并发, 必须是一个线程执行get后，内存里执行完毕ack后再取下一个get
 * </pre>
 * 
 * @since 1.1.1
 */
public class RocketMQCanalConnector implements CanalMQConnector {

    private static final Logger                 logger               = LoggerFactory.getLogger(RocketMQCanalConnector.class);
    private static final String                 CLOUD_ACCESS_CHANNEL = "cloud";

    private String                              nameServer;
    private String                              topic;
    private String                              groupName;
    private volatile boolean                    connected           = false;
    private DefaultMQPushConsumer               rocketMQConsumer;
    private BlockingQueue<ConsumerBatchMessage> messageBlockingQueue;
    private int                                 batchSize           = -1;
    private long                                batchProcessTimeout = 60 * 1000;
    private boolean                             flatMessage;
    private volatile ConsumerBatchMessage       lastGetBatchMessage = null;
    private String                              accessKey;
    private String                              secretKey;
    private String                              customizedTraceTopic;
    private boolean                             enableMessageTrace = false;
    private String                              accessChannel;
    private String                              namespace;

    public RocketMQCanalConnector(String nameServer, String topic, String groupName, String accessKey,
        String secretKey, Integer batchSize, boolean flatMessage, boolean enableMessageTrace,
        String customizedTraceTopic, String accessChannel, String namespace) {
        this(nameServer, topic, groupName, accessKey, secretKey, batchSize, flatMessage, enableMessageTrace, customizedTraceTopic, accessChannel);
        this.namespace = namespace;
    }

    public RocketMQCanalConnector(String nameServer, String topic, String groupName, String accessKey,
        String secretKey, Integer batchSize, boolean flatMessage, boolean enableMessageTrace,
        String customizedTraceTopic, String accessChannel) {
        this(nameServer, topic, groupName, accessKey, secretKey, batchSize, flatMessage);
        this.enableMessageTrace = enableMessageTrace;
        this.customizedTraceTopic = customizedTraceTopic;
        this.accessChannel = accessChannel;
    }

    public RocketMQCanalConnector(String nameServer, String topic, String groupName, Integer batchSize,
                                  boolean flatMessage){
        this.nameServer = nameServer;
        this.topic = topic;
        this.groupName = groupName;
        this.flatMessage = flatMessage;
        this.messageBlockingQueue = new LinkedBlockingQueue<>(1024);
        this.batchSize = batchSize;
    }

    public RocketMQCanalConnector(String nameServer, String topic, String groupName, String accessKey,
                                  String secretKey, Integer batchSize, boolean flatMessage){
        this(nameServer, topic, groupName, batchSize, flatMessage);
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public void connect() throws CanalClientException {
        RPCHook rpcHook = null;
        if (null != accessKey && accessKey.length() > 0 && null != secretKey && secretKey.length() > 0) {
            SessionCredentials sessionCredentials = new SessionCredentials();
            sessionCredentials.setAccessKey(accessKey);
            sessionCredentials.setSecretKey(secretKey);
            rpcHook = new AclClientRPCHook(sessionCredentials);
        }

        rocketMQConsumer = new DefaultMQPushConsumer(groupName, rpcHook, new AllocateMessageQueueAveragely(), enableMessageTrace, customizedTraceTopic);
        rocketMQConsumer.setVipChannelEnabled(false);
        if (CLOUD_ACCESS_CHANNEL.equals(this.accessChannel)) {
            rocketMQConsumer.setAccessChannel(AccessChannel.CLOUD);
        }

        if (!StringUtils.isEmpty(this.namespace)) {
            rocketMQConsumer.setNamespace(this.namespace);
        }

        if (!StringUtils.isBlank(nameServer)) {
            rocketMQConsumer.setNamesrvAddr(nameServer);
        }
        if (batchSize != -1) {
            rocketMQConsumer.setConsumeMessageBatchMaxSize(batchSize);
        }
    }

    public void disconnect() throws CanalClientException {
        rocketMQConsumer.shutdown();
        connected = false;
    }

    public boolean checkValid() throws CanalClientException {
        return connected;
    }

    public synchronized void subscribe(String filter) throws CanalClientException {
        if (connected) {
            return;
        }
        try {
            if (rocketMQConsumer == null) {
                this.connect();
            }
            rocketMQConsumer.subscribe(this.topic, "*");
            rocketMQConsumer.registerMessageListener(new MessageListenerOrderly() {

                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> messageExts, ConsumeOrderlyContext context) {
                    context.setAutoCommit(true);
                    boolean isSuccess = process(messageExts);
                    if (isSuccess) {
                        return ConsumeOrderlyStatus.SUCCESS;
                    } else {
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
            });
            rocketMQConsumer.start();
        } catch (MQClientException ex) {
            connected = false;
            logger.error("Start RocketMQ consumer error", ex);
        }
        connected = true;
    }

    private boolean process(List<MessageExt> messageExts) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get Message: {}", messageExts);
        }
        List messageList = Lists.newArrayList();
        for (MessageExt messageExt : messageExts) {
            byte[] data = messageExt.getBody();
            if (data != null) {
                try {
                    if (!flatMessage) {
                        Message message = CanalMessageDeserializer.deserializer(data);
                        messageList.add(message);
                    } else {
                        FlatMessage flatMessage = JSON.parseObject(data, FlatMessage.class);
                        messageList.add(flatMessage);
                    }
                } catch (Exception ex) {
                    logger.error("Add message error", ex);
                    throw new CanalClientException(ex);
                }
            } else {
                logger.warn("Received message data is null");
            }
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

    public void subscribe() throws CanalClientException {
        this.subscribe(null);
    }

    public void unsubscribe() throws CanalClientException {
        this.rocketMQConsumer.unsubscribe(this.topic);
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

    public Message get(int batchSize) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
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

}

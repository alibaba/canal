package com.alibaba.otter.canal.connector.rocketmq.consumer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.connector.core.config.CanalConstants;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.spi.CanalMsgConsumer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.connector.core.util.MessageUtil;
import com.alibaba.otter.canal.connector.rocketmq.config.RocketMQConstants;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;

/**
 * RocketMQ consumer SPI 实现
 *
 * @author rewerma 2020-02-01
 * @version 1.0.0
 */
@SPI("rocketmq")
public class CanalRocketMQConsumer implements CanalMsgConsumer {

    private static final Logger                                logger               = LoggerFactory.getLogger(CanalRocketMQConsumer.class);
    private static final String                                CLOUD_ACCESS_CHANNEL = "cloud";

    private String                                             nameServer;
    private String                                             topic;
    private String                                             groupName;
    private DefaultMQPushConsumer                              rocketMQConsumer;
    private BlockingQueue<ConsumerBatchMessage<CommonMessage>> messageBlockingQueue;
    private int                                                batchSize            = -1;
    private long                                               batchProcessTimeout  = 60 * 1000;
    private boolean                                            flatMessage;
    private volatile ConsumerBatchMessage<CommonMessage>       lastGetBatchMessage  = null;
    private String                                             accessKey;
    private String                                             secretKey;
    private String                                             customizedTraceTopic;
    private boolean                                            enableMessageTrace   = false;
    private String                                             accessChannel;
    private String                                             namespace;
    private String                                             filter               = "*";

    @Override
    public void init(Properties properties, String topic, String groupName) {
        this.topic = topic;
        this.groupName = groupName;
        this.flatMessage = (Boolean) properties.get(CanalConstants.CANAL_MQ_FLAT_MESSAGE);
        this.messageBlockingQueue = new LinkedBlockingQueue<>(1024);
        this.accessKey = properties.getProperty(CanalConstants.CANAL_ALIYUN_ACCESS_KEY);
        this.secretKey = properties.getProperty(CanalConstants.CANAL_ALIYUN_SECRET_KEY);
        String enableMessageTrace = properties.getProperty(RocketMQConstants.ROCKETMQ_ENABLE_MESSAGE_TRACE);
        if (StringUtils.isNotEmpty(enableMessageTrace)) {
            this.enableMessageTrace = Boolean.parseBoolean(enableMessageTrace);
        }
        this.customizedTraceTopic = properties.getProperty(RocketMQConstants.ROCKETMQ_CUSTOMIZED_TRACE_TOPIC);
        this.accessChannel = properties.getProperty(RocketMQConstants.ROCKETMQ_ACCESS_CHANNEL);
        this.namespace = properties.getProperty(RocketMQConstants.ROCKETMQ_NAMESPACE);
        this.nameServer = properties.getProperty(RocketMQConstants.ROCKETMQ_NAMESRV_ADDR);
        String batchSize = properties.getProperty(RocketMQConstants.ROCKETMQ_BATCH_SIZE);
        if (StringUtils.isNotEmpty(batchSize)) {
            this.batchSize = Integer.parseInt(batchSize);
        }
        String subscribeFilter = properties.getProperty(RocketMQConstants.ROCKETMQ_SUBSCRIBE_FILTER);
        if (StringUtils.isNotEmpty(subscribeFilter)) {
            this.filter = subscribeFilter;
        }
    }

    @Override
    public void connect() {
        RPCHook rpcHook = null;
        if (null != accessKey && accessKey.length() > 0 && null != secretKey && secretKey.length() > 0) {
            SessionCredentials sessionCredentials = new SessionCredentials();
            sessionCredentials.setAccessKey(accessKey);
            sessionCredentials.setSecretKey(secretKey);
            rpcHook = new AclClientRPCHook(sessionCredentials);
        }

        rocketMQConsumer = new DefaultMQPushConsumer(groupName,
            rpcHook,
            new AllocateMessageQueueAveragely(),
            enableMessageTrace,
            customizedTraceTopic);
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

        try {
            if (rocketMQConsumer == null) {
                this.connect();
            }
            rocketMQConsumer.subscribe(this.topic, this.filter);
            rocketMQConsumer.registerMessageListener((MessageListenerOrderly) (messageExts, context) -> {
                context.setAutoCommit(true);
                boolean isSuccess = process(messageExts);
                if (isSuccess) {
                    return ConsumeOrderlyStatus.SUCCESS;
                } else {
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            });
            rocketMQConsumer.start();
        } catch (MQClientException ex) {
            logger.error("Start RocketMQ consumer error", ex);
        }
    }

    private boolean process(List<MessageExt> messageExts) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get Message: {}", messageExts);
        }
        List<CommonMessage> messageList = Lists.newArrayList();
        for (MessageExt messageExt : messageExts) {
            byte[] data = messageExt.getBody();
            if (data != null) {
                try {
                    if (!flatMessage) {
                        Message message = CanalMessageSerializerUtil.deserializer(data);
                        messageList.addAll(MessageUtil.convert(message));
                    } else {
                        CommonMessage commonMessage = JSON.parseObject(data, CommonMessage.class);
                        messageList.add(commonMessage);
                    }
                } catch (Exception ex) {
                    logger.error("Add message error", ex);
                    throw new CanalClientException(ex);
                }
            } else {
                logger.warn("Received message data is null");
            }
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
        rocketMQConsumer.unsubscribe(topic);
        rocketMQConsumer.shutdown();
    }
}

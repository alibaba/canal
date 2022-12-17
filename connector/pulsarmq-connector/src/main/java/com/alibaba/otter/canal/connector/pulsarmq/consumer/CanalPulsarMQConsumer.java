package com.alibaba.otter.canal.connector.pulsarmq.consumer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.*;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.common.utils.MQUtil;
import com.alibaba.otter.canal.connector.core.config.CanalConstants;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.spi.CanalMsgConsumer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.connector.core.util.MessageUtil;
import com.alibaba.otter.canal.connector.pulsarmq.config.PulsarMQConstants;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;

/**
 * Pulsar consumer SPI 实现
 *
 * @author rewerma @ 2020-02-01
 * @version 1.0.0
 */
@SPI("pulsarmq")
public class CanalPulsarMQConsumer implements CanalMsgConsumer {

    /**
     * 连接pulsar客户端
     */
    private PulsarClient              pulsarClient;
    private Consumer<byte[]>          pulsarMQConsumer;
    /**
     * 是否为扁平消息
     */
    private boolean                   flatMessage            = false;
    /**
     * 主题名称
     */
    private String                    topic;
    /**
     * 单线程控制
     */
    private volatile Messages<byte[]> lastGetBatchMessage;
    /**
     * 环境连接URL
     */
    private String                    serviceUrl;
    /**
     * 角色认证token
     */
    private String                    roleToken;
    /**
     * 订阅客户端名称
     */
    private String                    subscriptName;
    /**
     * 每次批量获取数据的最大条目数，默认30
     */
    private int                       batchSize              = 30;
    /**
     * 与{@code batchSize}一起决定批量获取的数据大小 当：
     * <p>
     * 1. {@code batchSize} 条消息未消费时<br/>
     * 2. 距上一次批量消费时间达到{@code batchTimeoutSeconds}秒时
     * </p>
     * 任一条件满足，即执行批量消费
     */
    private int                       getBatchTimeoutSeconds = 30;
    /**
     * 批量处理消息时，一次批量处理的超时时间
     * <p>
     * 该时间应该根据{@code batchSize}和{@code batchTimeoutSeconds}合理设置
     * </p>
     */
    private long                      batchProcessTimeout    = 60 * 1000;
    /**
     * 消费失败后的重试秒数，默认60秒
     */
    private int                       redeliveryDelaySeconds = 60;
    /**
     * 当客户端接收到消息，30秒还没有返回ack给服务端时，ack超时，会重新消费该消息
     */
    private int                       ackTimeoutSeconds      = 30;
    /**
     * 是否开启消息失败重试功能，默认开启
     */
    private boolean                   isRetry                = true;
    /**
     * <p>
     * true重试(-RETRY)和死信队列(-DLQ)后缀为大写，有些地方创建的为小写，需确保正确
     * </p>
     */
    private boolean                   isRetryDLQUpperCase    = false;
    /**
     * 最大重试次数
     */
    private int                       maxRedeliveryCount     = 128;

    @Override
    public void init(Properties properties, String topic, String groupId) {
        this.topic = topic;
        String flatMessageStr = properties.getProperty(CanalConstants.CANAL_MQ_FLAT_MESSAGE);
        if (StringUtils.isNotEmpty(flatMessageStr)) {
            this.flatMessage = Boolean.parseBoolean(flatMessageStr);
        }
        this.serviceUrl = properties.getProperty(PulsarMQConstants.PULSARMQ_SERVER_URL);
        this.roleToken = properties.getProperty(PulsarMQConstants.PULSARMQ_ROLE_TOKEN);
        this.subscriptName = properties.getProperty(PulsarMQConstants.PULSARMQ_SUBSCRIPT_NAME);
        // 采用groupId作为subscriptName，避免所有的都是同一个订阅者名称
        if (StringUtils.isEmpty(this.subscriptName)) {
            this.subscriptName = groupId;
        }

        if (StringUtils.isEmpty(this.subscriptName)) {
            throw new RuntimeException("Pulsar Consumer subscriptName required");
        }
        String batchSizeStr = properties.getProperty(CanalConstants.CANAL_MQ_CANAL_BATCH_SIZE);
        if (StringUtils.isNotEmpty(batchSizeStr)) {
            this.batchSize = Integer.parseInt(batchSizeStr);
        }
        String getBatchTimeoutSecondsStr = properties.getProperty(PulsarMQConstants.PULSARMQ_GET_BATCH_TIMEOUT_SECONDS);
        if (StringUtils.isNotEmpty(getBatchTimeoutSecondsStr)) {
            this.getBatchTimeoutSeconds = Integer.parseInt(getBatchTimeoutSecondsStr);
        }
        String batchProcessTimeoutStr = properties.getProperty(PulsarMQConstants.PULSARMQ_BATCH_PROCESS_TIMEOUT);
        if (StringUtils.isNotEmpty(batchProcessTimeoutStr)) {
            this.batchProcessTimeout = Integer.parseInt(batchProcessTimeoutStr);
        }
        String redeliveryDelaySecondsStr = properties.getProperty(PulsarMQConstants.PULSARMQ_REDELIVERY_DELAY_SECONDS);
        if (StringUtils.isNotEmpty(redeliveryDelaySecondsStr)) {
            this.redeliveryDelaySeconds = Integer.parseInt(redeliveryDelaySecondsStr);
        }
        String ackTimeoutSecondsStr = properties.getProperty(PulsarMQConstants.PULSARMQ_ACK_TIMEOUT_SECONDS);
        if (StringUtils.isNotEmpty(ackTimeoutSecondsStr)) {
            this.ackTimeoutSeconds = Integer.parseInt(ackTimeoutSecondsStr);
        }
        String isRetryStr = properties.getProperty(PulsarMQConstants.PULSARMQ_IS_RETRY);
        if (StringUtils.isNotEmpty(isRetryStr)) {
            this.isRetry = Boolean.parseBoolean(isRetryStr);
        }
        String isRetryDLQUpperCaseStr = properties.getProperty(PulsarMQConstants.PULSARMQ_IS_RETRY_DLQ_UPPERCASE);
        if (StringUtils.isNotEmpty(isRetryDLQUpperCaseStr)) {
            this.isRetryDLQUpperCase = Boolean.parseBoolean(isRetryDLQUpperCaseStr);
        }
        String maxRedeliveryCountStr = properties.getProperty(PulsarMQConstants.PULSARMQ_MAX_REDELIVERY_COUNT);
        if (StringUtils.isNotEmpty(maxRedeliveryCountStr)) {
            this.maxRedeliveryCount = Integer.parseInt(maxRedeliveryCountStr);
        }
    }

    @Override
    public void connect() {
        if (isConsumerActive()) {
            return;
        }
        // 连接创建客户端
        try {
            // AuthenticationDataProvider
            ClientBuilder builder = PulsarClient.builder().serviceUrl(serviceUrl);
            if (StringUtils.isNotEmpty(roleToken)) {
                builder.authentication(AuthenticationFactory.token(roleToken));
            }
            pulsarClient = builder.build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer();
        if (MQUtil.isPatternTopic(this.topic)) {
            // 正则只支持一个
            builder.topicsPattern(this.topic);
        } else {// 多个topic
            builder.topic(this.topic);
        }
        // 为保证消息的有序性，仅支持单消费实例模式
        // 灾备模式，一个分区只能有一个消费者，如果当前消费者不可用，自动切换到其他消费者
        builder.subscriptionType(SubscriptionType.Failover);

        builder
            // 调用consumer.negativeAcknowledge(message) （即nack）来表示消费失败的消息
            // 在指定的时间进行重新消费，默认是1分钟。
            .negativeAckRedeliveryDelay(this.redeliveryDelaySeconds, TimeUnit.SECONDS)
            .subscriptionName(this.subscriptName);
        if (this.isRetry) {
            DeadLetterPolicy.DeadLetterPolicyBuilder dlqBuilder = DeadLetterPolicy.builder()
                // 最大重试次数
                .maxRedeliverCount(this.maxRedeliveryCount);
            // 指定重试队列，不是多个或通配符topic才能判断重试队列
            if (!MQUtil.isPatternTag(this.topic)) {
                String retryTopic = this.topic + (this.isRetryDLQUpperCase ? "-RETRY" : "-retry");
                dlqBuilder.retryLetterTopic(retryTopic);
                String dlqTopic = this.topic + (this.isRetryDLQUpperCase ? "-DLQ" : "-dlq");
                dlqBuilder.deadLetterTopic(dlqTopic);
            }

            // 默认关闭，如果需要重试则开启
            builder.enableRetry(true).deadLetterPolicy(dlqBuilder.build());
        }

        // ack超时
        builder.ackTimeout(this.ackTimeoutSeconds, TimeUnit.SECONDS);

        // pulsar批量获取消息设置
        builder.batchReceivePolicy(new BatchReceivePolicy.Builder().maxNumMessages(this.batchSize)
            .timeout(this.getBatchTimeoutSeconds, TimeUnit.SECONDS)
            .build());

        try {
            this.pulsarMQConsumer = builder.subscribe();
        } catch (PulsarClientException e) {
            throw new CanalClientException("Subscript pulsar consumer error", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<CommonMessage> getMessage(Long timeout, TimeUnit unit) {
        List<CommonMessage> messageList = Lists.newArrayList();
        try {
            Messages<byte[]> messages = pulsarMQConsumer.batchReceive();
            if (null == messages || messages.size() == 0) {
                return messageList;
            }
            // 保存当前消费记录，用于ack和rollback
            this.lastGetBatchMessage = messages;
            for (org.apache.pulsar.client.api.Message<byte[]> msg : messages) {
                byte[] data = msg.getData();
                if (!this.flatMessage) {
                    Message message = CanalMessageSerializerUtil.deserializer(data);
                    List<CommonMessage> list = MessageUtil.convert(message);
                    messageList.addAll(list);
                } else {
                    CommonMessage commonMessage = JSON.parseObject(data, CommonMessage.class);
                    messageList.add(commonMessage);
                }
            }
        } catch (PulsarClientException e) {
            throw new CanalClientException("Receive pulsar batch message error", e);
        }

        return messageList;
    }

    @Override
    public void rollback() {
        try {
            if (isConsumerActive() && hasLastMessages()) {
                // 回滚所有消息
                this.pulsarMQConsumer.negativeAcknowledge(this.lastGetBatchMessage);
            }
        } finally {
            this.lastGetBatchMessage = null;
        }
    }

    @Override
    public void ack() {
        try {
            if (isConsumerActive() && hasLastMessages()) {
                // 确认所有消息
                this.pulsarMQConsumer.acknowledge(this.lastGetBatchMessage);
            }
        } catch (PulsarClientException e) {
            if (isConsumerActive() && hasLastMessages()) {
                this.pulsarMQConsumer.negativeAcknowledge(this.lastGetBatchMessage);
            }
        } finally {
            this.lastGetBatchMessage = null;
        }
    }

    @Override
    public void disconnect() {
        if (null == this.pulsarMQConsumer || !this.pulsarMQConsumer.isConnected()) {
            return;
        }
        try {
            // 会导致暂停期间数据丢失
            // this.pulsarMQConsumer.unsubscribe();
            this.pulsarClient.close();
        } catch (PulsarClientException e) {
            throw new CanalClientException("Disconnect pulsar consumer error", e);
        }
    }

    /**
     * 是否消费可用
     *
     * @return true消费者可用
     */
    private boolean isConsumerActive() {
        return null != this.pulsarMQConsumer && this.pulsarMQConsumer.isConnected();
    }

    /**
     * 是否有未确认消息
     *
     * @return true有正在消费的待确认消息
     */
    private boolean hasLastMessages() {
        return null != this.lastGetBatchMessage && this.lastGetBatchMessage.size() > 0;
    }
}

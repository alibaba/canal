package com.alibaba.otter.canal.client.pulsarmq;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.common.utils.MQUtil;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * PulsarMQ的连接
 *
 * <pre>
 * 注意点:
 * 1. 相比于canal {@linkplain SimpleCanalConnector}, 这里get和ack操作不能有并发, 必须是一个线程执行get后，内存里执行完毕ack后再取下一个get
 * </pre>
 * todo 重复消费的概率相当高。一次批处理中，只要有一个消息处理失败，则该批次全部重试
 *
 * @since 1.1.1
 */
public class PulsarMQCanalConnector implements CanalMQConnector {

    private static final Logger logger = LoggerFactory.getLogger(PulsarMQCanalConnector.class);

    private volatile Messages<byte[]> lastGetBatchMessage;

    /**
     * 连接pulsar客户端
     */
    private PulsarClient pulsarClient;
    /**
     * 消费者
     */
    private Consumer<byte[]> consumer;
    /**
     * 是否扁平化Canal消息内容
     */
    private boolean isFlatMessage = false;
    /**
     * 主题名称
     */
    private String topic;
    /**
     * 环境连接URL
     */
    private String serviceUrl;
    /**
     * 角色认证token
     */
    private String roleToken;
    /**
     * 订阅客户端名称
     */
    private String subscriptName;
    /**
     * 每次批量获取数据的最大条目数，默认30
     */
    private int batchSize = 30;
    /**
     * 与{@code batchSize}一起决定批量获取的数据大小
     * 当：
     * <p>
     * 1. {@code batchSize} 条消息未消费时<br/>
     * 2. 距上一次批量消费时间达到{@code batchTimeoutSeconds}秒时
     * </p>
     * 任一条件满足，即执行批量消费
     */
    private int getBatchTimeoutSeconds = 30;
    /**
     * 批量处理消息时，一次批量处理的超时时间秒数
     * <p>
     * 该时间应该根据{@code batchSize}和{@code batchTimeoutSeconds}合理设置
     * </p>
     */
    private int batchProcessTimeoutSeconds = 60;
    /**
     * 消费失败后的重试秒数，默认60秒
     */
    private int redeliveryDelaySeconds = 60;
    /**
     * 当客户端接收到消息，30秒还没有返回ack给服务端时，ack超时，会重新消费该消息
     */
    private int ackTimeoutSeconds = 30;
    /**
     * 是否开启消息失败重试功能，默认开启
     */
    private boolean isRetry = true;
    /**
     * <p>
     * true重试(-RETRY)和死信队列(-DLQ)后缀为大写，有些地方创建的为小写，需确保正确
     * </p>
     */
    private boolean isRetryDLQUpperCase = false;
    /**
     * 最大重试次数
     */
    private int maxRedeliveryCount = 128;
    /**
     * 连接标识位，在连接或关闭连接后改变值
     */
    private boolean connected = false;

    /**
     * 除必要参数外，其他参数使用默认值
     * <p>
     * 由于pulsar会根据subscriptName来区分消费实例，并且已经分配的指定实例的消息会固定到该实例的retry（重试）和dlq（死信）队列中，
     * 所以subscriptName必传，且必须跟之前的一致，否则会导致之前消费失败的消息不会重消费。
     * </p>
     *
     * @param isFlatMessage true使用扁平消息
     * @param serviceUrl    pulsar服务连接地址，通常为：pulsar:host:ip或http://host:ip
     * @param roleToken     有对应topic的消费者权限的角色token
     * @param topic         订阅主题
     * @param subscriptName 订阅和客户端名称，同一个订阅名视为同一个消费实例
     * @date 2021/9/18 08:54
     * @author chad
     * @since 1 by chad at 2021/9/18 完善
     */
    public PulsarMQCanalConnector(boolean isFlatMessage, String serviceUrl, String roleToken, String topic
            , String subscriptName) {
        this.isFlatMessage = isFlatMessage;
        this.serviceUrl = serviceUrl;
        this.roleToken = roleToken;
        this.topic = topic;
        this.subscriptName = subscriptName;
        if (StringUtils.isEmpty(this.subscriptName)) {
            throw new RuntimeException("Pulsar Consumer subscriptName required");
        }
    }

    /**
     * 完全自定义的消费实例参数
     *
     * @date 2021/9/18 10:20
     * @author chad
     * @since 1 by chad at 2021/9/18 完善
     */
    public PulsarMQCanalConnector(boolean isFlatMessage, String serviceUrl, String roleToken, String topic
            , String subscriptName, int batchSize, int getBatchTimeoutSeconds, int batchProcessTimeoutSeconds
            , int redeliveryDelaySeconds, int ackTimeoutSeconds, boolean isRetry, boolean isRetryDLQUpperCase
            , int maxRedeliveryCount) {
        this.isFlatMessage = isFlatMessage;
        this.serviceUrl = serviceUrl;
        this.roleToken = roleToken;
        this.topic = topic;
        this.subscriptName = subscriptName;
        if (StringUtils.isEmpty(this.subscriptName)) {
            throw new RuntimeException("Pulsar Consumer subscriptName required");
        }
        this.batchSize = batchSize;
        this.getBatchTimeoutSeconds = getBatchTimeoutSeconds;
        this.batchProcessTimeoutSeconds = batchProcessTimeoutSeconds;
        this.redeliveryDelaySeconds = redeliveryDelaySeconds;
        this.ackTimeoutSeconds = ackTimeoutSeconds;
        this.isRetry = isRetry;
        this.isRetryDLQUpperCase = isRetryDLQUpperCase;
        this.maxRedeliveryCount = maxRedeliveryCount;
    }

    @Override
    public void connect() throws CanalClientException {
        // 连接创建客户端
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .authentication(AuthenticationFactory.token(roleToken))
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void disconnect() throws CanalClientException {
        try {
            if (null != this.consumer && this.consumer.isConnected()) {
                this.consumer.close();
            }
        } catch (PulsarClientException e) {
            logger.error("close pulsar consumer error", e);
        }
        try {
            if (null != this.pulsarClient) {
                this.pulsarClient.close();
            }
        } catch (PulsarClientException e) {
            logger.error("close pulsar client error", e);
        }

        this.connected = false;
    }

    @Override
    public boolean checkValid() throws CanalClientException {
        return connected;
    }

    @Override
    public synchronized void subscribe(String filter) throws CanalClientException {
        if (connected) {
            return;
        }
        ConsumerBuilder<byte[]> builder = pulsarClient.newConsumer();

        if (MQUtil.isPatternTopic(this.topic)) {
            // 正则
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
                .subscriptionName(this.subscriptName)
        ;
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

            //默认关闭，如果需要重试则开启
            builder.enableRetry(true)
                    .deadLetterPolicy(dlqBuilder.build());
        }

        // ack超时
        builder.ackTimeout(this.ackTimeoutSeconds, TimeUnit.SECONDS);

        // pulsar批量获取消息设置
        builder.batchReceivePolicy(new BatchReceivePolicy.Builder()
                .maxNumMessages(this.batchSize)
                .timeout(this.getBatchTimeoutSeconds, TimeUnit.SECONDS)
                .build());

        try {
            this.consumer = builder.subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        connected = true;
    }


    @Override
    public void subscribe() throws CanalClientException {
        this.subscribe(null);
    }

    @Override
    public void unsubscribe() throws CanalClientException {
        try {
            if (null != this.consumer) {
                this.consumer.unsubscribe();
            }
        } catch (PulsarClientException e) {
            throw new CanalClientException(e.getMessage(), e);
        }
    }

    /**
     * 不关注业务执行结果，只要收到消息即认识消费成功，自动ack
     *
     * @param timeout 阻塞获取消息的超时时间
     * @param unit    时间单位
     * @return java.util.List<com.alibaba.otter.canal.protocol.Message>
     * @date 2021/9/13 22:24
     * @author chad
     * @since 1 by chad at 2021/9/13 添加注释
     */
    @Override
    public List<Message> getList(Long timeout, TimeUnit unit) throws CanalClientException {
        List<Message> messages = getListWithoutAck(timeout, unit);
        if (messages != null && !messages.isEmpty()) {
            ack();
        }
        return messages;
    }

    /**
     * 关心业务执行结果，业务侧根据执行结果调用 {@link PulsarMQCanalConnector#ack()}或{@link PulsarMQCanalConnector#rollback()}
     * <p>
     * 本方法示支持多线程，在MQ保障顺序的前提下，也无法提供单Topic多线程
     * </p>
     *
     * @param timeout 阻塞获取消息的超时时间
     * @param unit    时间单位
     * @return java.util.List<com.alibaba.otter.canal.protocol.Message>
     * @date 2021/9/13 22:26
     * @author chad
     * @since 1 by chad at 2021/9/13 添加注释
     */
    @Override
    public List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        return getListWithoutAck();
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
        return getListWithoutAck();
    }

    /**
     * 获取泛型数据，供其他方法调用
     * <p>
     * 不支持多线程调用
     * </p>
     *
     * @return java.util.List<T>
     * @date 2021/9/14 15:20
     * @author chad
     * @since 1 by chad at 2021/9/14 供{@link PulsarMQCanalConnector#getListWithoutAck(Long, TimeUnit)}
     * 和{@link PulsarMQCanalConnector#getFlatListWithoutAck(Long, TimeUnit)}调用
     */
    private <T> List<T> getListWithoutAck() {
        if (null != this.lastGetBatchMessage) {
            throw new CanalClientException("mq get/ack not support concurrent & async ack");
        }
        List messageList = Lists.newArrayList();

        try {
            this.lastGetBatchMessage = consumer.batchReceive();
            if (null == this.lastGetBatchMessage || this.lastGetBatchMessage.size() < 1) {
                this.lastGetBatchMessage = null;
                return messageList;
            }
        } catch (PulsarClientException e) {
            logger.error("Receiver Pulsar MQ message error", e);
            throw new CanalClientException(e);
        }

        for (org.apache.pulsar.client.api.Message<byte[]> msgExt : this.lastGetBatchMessage) {
            byte[] data = msgExt.getData();
            if (data == null) {
                logger.warn("Received message data is null");
                continue;
            }
            try {
                if (isFlatMessage) {
                    FlatMessage flatMessage = JSON.parseObject(data, FlatMessage.class);
                    messageList.add(flatMessage);
                } else {
                    Message message = CanalMessageDeserializer.deserializer(data);
                    messageList.add(message);
                }
            } catch (Exception ex) {
                logger.error("Add message error", ex);
                throw new CanalClientException(ex);
            }
        }

        return messageList;
    }

    /**
     * 当业务侧执行成功时，需要手动执行消息的ack操作
     *
     * @return void
     * @date 2021/9/13 22:27
     * @author chad
     * @since 1 by chad at 2021/9/13 添加注释
     */
    @Override
    public void ack() throws CanalClientException {
        // 为什么要一个批次要么全部成功要么全部失败
        try {
            if (this.lastGetBatchMessage != null) {
                this.consumer.acknowledge(this.lastGetBatchMessage);
            }
        } catch (Throwable e) {
            if (this.lastGetBatchMessage != null) {
                this.consumer.negativeAcknowledge(this.lastGetBatchMessage);
            }
        } finally {
            this.lastGetBatchMessage = null;
        }
    }

    /**
     * 当业务侧执行失败时，需要手动执行消息的rollback操作，从而让消息重新消费
     *
     * @return void
     * @date 2021/9/13 22:28
     * @author chad
     * @since 1 by chad at 2021/9/13 添加注释
     */
    @Override
    public void rollback() throws CanalClientException {
        try {
            if (this.lastGetBatchMessage != null) {
                this.consumer.negativeAcknowledge(this.lastGetBatchMessage);
            }
        } finally {
            this.lastGetBatchMessage = null;
        }
    }

    @Override
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

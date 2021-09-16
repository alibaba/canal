package com.alibaba.otter.canal.connector.pulsarmq.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.common.utils.PropertiesUtils;
import com.alibaba.otter.canal.connector.core.producer.AbstractMQProducer;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.producer.MQMessageUtils;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.connector.pulsarmq.config.PulsarMQConstants;
import com.alibaba.otter.canal.connector.pulsarmq.config.PulsarMQProducerConfig;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * PulsarMQ Producer SPI 实现
 *
 * @author chad 2021/9/2
 * @version 1.0.0
 */
@SPI("pulsarmq")
public class CanalPulsarMQProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalPulsarMQProducer.class);

    private static final Map<String, Producer<byte[]>> PRODUCERS = new HashMap<>();

    protected ThreadPoolExecutor sendPartitionExecutor;
    /**
     * pulsar客户端，管理连接
     */
    protected PulsarClient client;

    @Override
    public void init(Properties properties) {
        // 加载配置
        PulsarMQProducerConfig pulsarMQProducerConfig = new PulsarMQProducerConfig();
        this.mqProperties = pulsarMQProducerConfig;
        super.init(properties);
        loadPulsarMQProperties(properties);

        // 初始化连接客户端
        try {
            client = PulsarClient.builder()
                    // 填写pulsar的连接地址
                    .serviceUrl(pulsarMQProducerConfig.getServerUrl())
                    // 角色权限认证的token
                    .authentication(AuthenticationFactory.token(pulsarMQProducerConfig.getRoleToken()))
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        // 加载所有生产者 --> topic可能有正则或表名，无法确认所有topic，在使用时再加载

        int parallelPartitionSendThreadSize = mqProperties.getParallelSendThreadSize();
        sendPartitionExecutor = new ThreadPoolExecutor(parallelPartitionSendThreadSize,
                parallelPartitionSendThreadSize,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(parallelPartitionSendThreadSize * 2),
                new NamedThreadFactory("MQ-Parallel-Sender-Partition"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 加载配置
     *
     * @param properties
     * @return void
     * @date 2021/9/15 11:22
     * @author chad
     * @since 1 by chad at 2021/9/15 新增
     */
    private void loadPulsarMQProperties(Properties properties) {
        PulsarMQProducerConfig tmpProperties = (PulsarMQProducerConfig) this.mqProperties;
        String serverUrl = PropertiesUtils.getProperty(properties, PulsarMQConstants.PULSARMQ_SERVER_URL);
        if (!StringUtils.isEmpty(serverUrl)) {
            tmpProperties.setServerUrl(serverUrl);
        }

        String roleToken = PropertiesUtils.getProperty(properties, PulsarMQConstants.PULSARMQ_ROLE_TOKEN);
        if (!StringUtils.isEmpty(roleToken)) {
            tmpProperties.setRoleToken(roleToken);
        }
        String topicTenantPrefix = PropertiesUtils.getProperty(properties, PulsarMQConstants.PULSARMQ_TOPIC_TENANT_PREFIX);
        if (!StringUtils.isEmpty(topicTenantPrefix)) {
            tmpProperties.setTopicTenantPrefix(topicTenantPrefix);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Load pulsar properties ==> {}", JSON.toJSON(this.mqProperties));
        }
    }

    /**
     * 发送消息，处理的任务：
     * <p>
     * 1. 动态 Topic，根据schema.table或schema来匹配topic配置，将改变发送到指定的一个或多个具体的Topic<br/>
     * 2. 使用线程池发送多个消息，单个消息不使用线程池
     * </p>
     *
     * @param destination 消息目标信息
     * @param message     消息
     * @param callback    消息发送结果回调
     * @return void
     * @date 2021/9/2 22:01
     * @author chad
     * @since 1.0.0 by chad at 2021/9/2: 新增
     */
    @Override
    public void send(MQDestination destination, com.alibaba.otter.canal.protocol.Message message, Callback callback) {

        ExecutorTemplate template = new ExecutorTemplate(sendExecutor);
        try {
            if (!StringUtils.isEmpty(destination.getDynamicTopic())) {
                // 动态topic
                Map<String, com.alibaba.otter.canal.protocol.Message> messageMap = MQMessageUtils.messageTopics(message,
                        destination.getTopic(),
                        destination.getDynamicTopic());

                for (Map.Entry<String, com.alibaba.otter.canal.protocol.Message> entry : messageMap.entrySet()) {
                    String topicName = entry.getKey().replace('.', '_');
                    com.alibaba.otter.canal.protocol.Message messageSub = entry.getValue();
                    template.submit(() -> {
                        try {
                            send(destination, topicName, messageSub);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
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

    /**
     * 发送单条消息到指定topic。区分是否发送扁平消息
     *
     * @param destination
     * @param topicName
     * @param message
     * @return void
     * @date 2021/9/2 22:05
     * @author chad
     * @since 1.0.0 by chad at 2021/9/2: 新增
     */
    public void send(final MQDestination destination, String topicName, com.alibaba.otter.canal.protocol.Message message) {

        // 获取当前topic的分区数
        Integer partitionNum = MQMessageUtils.parseDynamicTopicPartition(topicName,
                destination.getDynamicTopicPartitionNum());
        if (partitionNum == null) {
            partitionNum = destination.getPartitionsNum();
        }
        ExecutorTemplate template = new ExecutorTemplate(sendPartitionExecutor);
        // 并发构造
        MQMessageUtils.EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
        if (!mqProperties.isFlatMessage()) {
            // 串行发送

            for (MQMessageUtils.EntryRowData r : datas) {
                CanalEntry.Entry entry = r.entry;
                if (null == entry) {
                    continue;
                }

                template.submit(() -> {
                    sendMessage(topicName, message.getId(), entry);
                });

            }
        } else {
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, message.getId());
            template.submit(() -> {
                sendMessage(topicName, flatMessages);
            });
        }
    }

    /**
     * 发送原始消息，需要做分区处理
     *
     * @param topic topic
     * @param msgId 消息ID
     * @param entry 原始消息内容
     * @return void
     * @date 2021/9/10 17:55
     * @author chad
     * @since 1 by chad at 2021/9/10 新增
     */
    private void sendMessage(String topic, long msgId, CanalEntry.Entry entry) {
        Producer<byte[]> producer = getProducer(topic);
        List<CanalEntry.Entry> entries = new ArrayList<>(1);
        entries.add(entry);
        com.alibaba.otter.canal.protocol.Message msg = new com.alibaba.otter.canal.protocol.Message(msgId, entries);
        byte[] msgBytes = CanalMessageSerializerUtil.serializer(msg, mqProperties.isFilterTransactionEntry());
        try {
            MessageId msgResultId = producer.newMessage().value(msgBytes).send();
            // todo 判断发送结果
            if (logger.isDebugEnabled()) {
                logger.debug("Send Message to topic:{} Result: {}", topic, msgResultId);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 发送扁平消息
     *
     * @param topic        topic主题
     * @param flatMessages 扁平消息
     * @return void
     * @date 2021/9/10 18:22
     * @author chad
     * @since 1 by chad at 2021/9/10 新增
     */
    private void sendMessage(String topic, List<FlatMessage> flatMessages) {
        Producer<byte[]> producer = getProducer(topic);
        for (FlatMessage f : flatMessages) {
            try {
                MessageId msgResultId = producer.newMessage().value(JSON.toJSONBytes(f, SerializerFeature.WriteMapNullValue)).send();
                // todo 判断发送结果
                if (logger.isDebugEnabled()) {
                    logger.debug("Send Messages to topic:{} Result: {}", topic, msgResultId);
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 获取指定topic的生产者，并且使用缓存
     *
     * @param topic
     * @return org.apache.pulsar.client.api.Producer<byte [ ]>
     * @date 2021/9/10 11:21
     * @author chad
     * @since 1 by chad at 2021/9/10 新增
     */
    private Producer<byte[]> getProducer(String topic) {
        Producer producer = PRODUCERS.get(topic);

        if (null == producer) {
            try {
                synchronized (PRODUCERS) {
                    producer = PRODUCERS.get(topic);
                    if (null != producer) {
                        return producer;
                    }

                    // 拼接topic前缀
                    PulsarMQProducerConfig pulsarMQProperties = (PulsarMQProducerConfig) this.mqProperties;
                    String prefix = pulsarMQProperties.getTopicTenantPrefix();
                    String fullTopic = topic;
                    if (!StringUtils.isEmpty(prefix)) {
                        if (!prefix.endsWith("/")) {
                            fullTopic = "/" + fullTopic;
                        }
                        fullTopic = pulsarMQProperties.getTopicTenantPrefix() + fullTopic;
                    }

                    // 创建指定topic的生产者
                    producer = client.newProducer()
                            .topic(fullTopic)
                            // 指定路由器
                            .messageRouter(mqProperties.isFlatMessage() ? new FlagMessageRouterImpl() : new MessageRouterImpl())
                            .create();
                    // 放入缓存
                    PRODUCERS.put(topic, producer);
                }
            } catch (PulsarClientException e) {
                logger.error("create producer failed for topic: " + topic, e);
                throw new RuntimeException(e);
            }
        }

        return producer;
    }

    /**
     * Pulsar自定义路由策略
     *
     * @author chad
     * @version 1
     * @since 1 by chad at 2021/9/10 新增
     */
    private static class MessageRouterImpl implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            // todo 原始Entry消息分区
            return 0;
        }
    }

    /**
     * Pulsar扁平消息自定义路由策略
     *
     * @author chad
     * @version 1
     * @since 1 by chad at 2021/9/10 新增
     */
    private static class FlagMessageRouterImpl implements MessageRouter {
        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            // 获取当前topic的分区数
            Integer partitionNum = metadata.numPartitions();
            // todo 扁平消息分区
            return 0;
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop RocketMQ producer##");

        for (Producer p : PRODUCERS.values()) {
            try {
                if (null != p && p.isConnected()) {
                    p.close();
                }
            } catch (PulsarClientException e) {
                logger.warn("close producer name: {}, topic: {}, error: {}", p.getProducerName(), p.getTopic(), e.getMessage());
            }
        }

        super.stop();
    }
}

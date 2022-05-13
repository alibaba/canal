package com.alibaba.otter.canal.connector.pulsarmq.producer;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
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

/**
 * PulsarMQ Producer SPI 实现
 *
 * @author chad 2021/9/2
 * @version 1.0.0
 */
@SPI("pulsarmq")
public class CanalPulsarMQProducer extends AbstractMQProducer implements CanalMQProducer {

    /**
     * 消息体分区属性名称
     */
    public static final String                         MSG_PROPERTY_PARTITION_NAME = "partitionNum";
    private static final Logger                        logger                      = LoggerFactory
        .getLogger(CanalPulsarMQProducer.class);
    private static final Map<String, Producer<byte[]>> PRODUCERS                   = new HashMap<>();
    protected ThreadPoolExecutor                       sendPartitionExecutor;
    /**
     * pulsar客户端，管理连接
     */
    protected PulsarClient                             client;
    /**
     * Pulsar admin 客户端
     */
    protected PulsarAdmin                              pulsarAdmin;

    @Override
    public void init(Properties properties) {
        // 加载配置
        PulsarMQProducerConfig pulsarMQProducerConfig = new PulsarMQProducerConfig();
        this.mqProperties = pulsarMQProducerConfig;
        super.init(properties);
        loadPulsarMQProperties(properties);

        // 初始化连接客户端
        try {
            ClientBuilder builder = PulsarClient.builder()
                // 填写pulsar的连接地址
                .serviceUrl(pulsarMQProducerConfig.getServerUrl());
            if (StringUtils.isNotEmpty(pulsarMQProducerConfig.getRoleToken())) {
                // 角色权限认证的token
                builder.authentication(AuthenticationFactory.token(pulsarMQProducerConfig.getRoleToken()));
            }
            client = builder.build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        // 初始化Pulsar admin
        if (StringUtils.isNotEmpty(pulsarMQProducerConfig.getAdminServerUrl())) {
            try {
                pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsarMQProducerConfig.getAdminServerUrl()).build();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
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
        String topicTenantPrefix = PropertiesUtils.getProperty(properties,
            PulsarMQConstants.PULSARMQ_TOPIC_TENANT_PREFIX);
        if (!StringUtils.isEmpty(topicTenantPrefix)) {
            tmpProperties.setTopicTenantPrefix(topicTenantPrefix);
        }
        String adminServerUrl = PropertiesUtils.getProperty(properties, PulsarMQConstants.PULSARMQ_ADMIN_SERVER_URL);
        if (!StringUtils.isEmpty(adminServerUrl)) {
            tmpProperties.setAdminServerUrl(adminServerUrl);
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
     * @param message 消息
     * @param callback 消息发送结果回调
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
                Map<String, com.alibaba.otter.canal.protocol.Message> messageMap = MQMessageUtils
                    .messageTopics(message, destination.getTopic(), destination.getDynamicTopic());

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
    public void send(final MQDestination destination, String topicName,
                     com.alibaba.otter.canal.protocol.Message message) {

        // 获取当前topic的分区数
        Integer partitionNum = MQMessageUtils.parseDynamicTopicPartition(topicName,
            destination.getDynamicTopicPartitionNum());
        if (partitionNum == null) {
            partitionNum = destination.getPartitionsNum();
        }
        // 创建多分区topic
        if (pulsarAdmin != null && partitionNum != null && partitionNum > 0 && PRODUCERS.get(topicName) == null) {
            createMultipleTopic(topicName, partitionNum);
        }

        ExecutorTemplate template = new ExecutorTemplate(sendPartitionExecutor);
        // 并发构造
        MQMessageUtils.EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
        if (!mqProperties.isFlatMessage()) {
            // 动态计算目标分区
            if (destination.getPartitionHash() != null && !destination.getPartitionHash().isEmpty()) {
                for (MQMessageUtils.EntryRowData r : datas) {
                    CanalEntry.Entry entry = r.entry;
                    if (null == entry) {
                        continue;
                    }
                    // 串行分区
                    com.alibaba.otter.canal.protocol.Message[] messages = MQMessageUtils.messagePartition(datas,
                        message.getId(),
                        partitionNum,
                        destination.getPartitionHash(),
                        mqProperties.isDatabaseHash());
                    // 发送
                    int len = messages.length;
                    for (int i = 0; i < len; i++) {
                        final int partition = i;
                        com.alibaba.otter.canal.protocol.Message m = messages[i];
                        template.submit(() -> {
                            sendMessage(topicName, partition, m);
                        });
                    }
                }
            } else {
                // 默认分区
                final int partition = destination.getPartition() != null ? destination.getPartition() : 0;
                sendMessage(topicName, partition, message);
            }
        } else {
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, message.getId());

            // 初始化分区合并队列
            if (destination.getPartitionHash() != null && !destination.getPartitionHash().isEmpty()) {
                List<List<FlatMessage>> partitionFlatMessages = new ArrayList<>();
                int len = partitionNum;
                for (int i = 0; i < len; i++) {
                    partitionFlatMessages.add(new ArrayList<>());
                }

                for (FlatMessage flatMessage : flatMessages) {
                    FlatMessage[] partitionFlatMessage = MQMessageUtils.messagePartition(flatMessage,
                        partitionNum,
                        destination.getPartitionHash(),
                        mqProperties.isDatabaseHash());
                    int length = partitionFlatMessage.length;
                    for (int i = 0; i < length; i++) {
                        // 增加null判断,issue #3267
                        if (partitionFlatMessage[i] != null) {
                            partitionFlatMessages.get(i).add(partitionFlatMessage[i]);
                        }
                    }
                }

                for (int i = 0; i < len; i++) {
                    final List<FlatMessage> flatMessagePart = partitionFlatMessages.get(i);
                    if (flatMessagePart != null && flatMessagePart.size() > 0) {
                        final int partition = i;
                        template.submit(() -> {
                            // 批量发送
                            sendMessage(topicName, partition, flatMessagePart);
                        });
                    }
                }

                // 批量等所有分区的结果
                template.waitForResult();
            } else {
                // 默认分区
                final int partition = destination.getPartition() != null ? destination.getPartition() : 0;
                sendMessage(topicName, partition, flatMessages);
            }
        }
    }

    /**
     * 发送原始消息，需要做分区处理
     *
     * @param topic topic
     * @param partitionNum 目标分区
     * @param msg 原始消息内容
     * @return void
     * @date 2021/9/10 17:55
     * @author chad
     * @since 1 by chad at 2021/9/10 新增
     */
    private void sendMessage(String topic, int partitionNum, com.alibaba.otter.canal.protocol.Message msg) {
        Producer<byte[]> producer = getProducer(topic);
        byte[] msgBytes = CanalMessageSerializerUtil.serializer(msg, mqProperties.isFilterTransactionEntry());
        try {
            MessageId msgResultId = producer.newMessage()
                .property(MSG_PROPERTY_PARTITION_NAME, String.valueOf(partitionNum))
                .value(msgBytes)
                .send();
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
     * @param topic topic主题
     * @param flatMessages 扁平消息
     * @return void
     * @date 2021/9/10 18:22
     * @author chad
     * @since 1 by chad at 2021/9/10 新增
     */
    private void sendMessage(String topic, int partition, List<FlatMessage> flatMessages) {
        Producer<byte[]> producer = getProducer(topic);
        for (FlatMessage f : flatMessages) {
            try {
                MessageId msgResultId = producer.newMessage()
                    .property(MSG_PROPERTY_PARTITION_NAME, String.valueOf(partition))
                    .value(JSON.toJSONBytes(f, Feature.WriteNulls))
                    .send()
                //
                ;
                if (logger.isDebugEnabled()) {
                    logger.debug("Send Messages to topic:{} Result: {}", topic, msgResultId);
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 创建多分区topic
     * 
     * @param topic
     * @param partitionNum
     */
    private void createMultipleTopic(String topic, Integer partitionNum) {
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

        // 创建分区topic
        try {
            pulsarAdmin.topics().createPartitionedTopic(fullTopic, partitionNum);
        } catch (PulsarAdminException e) {
            // TODO 无论是否报错，都继续后续的操作，此处不进行阻塞
        }
    }

    /**
     * 获取topic
     * 
     * @param topic
     * @return
     */
    private Producer<byte[]> getProducer(String topic) {
        Producer producer = PRODUCERS.get(topic);
        if (null == producer || !producer.isConnected()) {
            try {
                synchronized (PRODUCERS) {
                    producer = PRODUCERS.get(topic);
                    if (null != producer && producer.isConnected()) {
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
                        .messageRouter(new MessageRouterImpl(topic))
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

    @Override
    public void stop() {
        logger.info("## Stop PulsarMQ producer##");

        for (Producer p : PRODUCERS.values()) {
            try {
                if (null != p && p.isConnected()) {
                    p.close();
                }
            } catch (PulsarClientException e) {
                logger.warn("close producer name: {}, topic: {}, error: {}",
                    p.getProducerName(),
                    p.getTopic(),
                    e.getMessage());
            }
        }

        super.stop();
    }

    /**
     * Pulsar自定义路由策略
     *
     * @author chad
     * @version 1
     * @since 1 by chad at 2021/9/10 新增
     * @since 2 by chad at 2021/9/17 修改为msg自带目标分区
     */
    private static class MessageRouterImpl implements MessageRouter {

        private String topicLocal;

        public MessageRouterImpl(String topicLocal){
            this.topicLocal = topicLocal;
        }

        @Override
        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
            String partitionStr = msg.getProperty(MSG_PROPERTY_PARTITION_NAME);
            int partition = 0;
            if (!StringUtils.isEmpty(partitionStr)) {
                try {
                    partition = Integer.parseInt(partitionStr);
                } catch (NumberFormatException e) {
                    logger
                        .warn("Parse msg {} property failed for value: {}", MSG_PROPERTY_PARTITION_NAME, partitionStr);
                }
            }
            // topic创建时设置的分区数
            Integer partitionNum = metadata.numPartitions();
            // 如果 partition 超出 partitionNum，取余数
            if (null != partitionNum && partition >= partitionNum) {
                partition = partition % partitionNum;
            }
            return partition;
        }
    }
}

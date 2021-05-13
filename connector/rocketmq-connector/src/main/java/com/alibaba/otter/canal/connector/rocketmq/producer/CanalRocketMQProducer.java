package com.alibaba.otter.canal.connector.rocketmq.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.CanalException;
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
import com.alibaba.otter.canal.connector.rocketmq.config.RocketMQConstants;
import com.alibaba.otter.canal.connector.rocketmq.config.RocketMQProducerConfig;
import com.alibaba.otter.canal.protocol.FlatMessage;

/**
 * RocketMQ Producer SPI 实现
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
@SPI("rocketmq")
public class CanalRocketMQProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger  logger               = LoggerFactory.getLogger(CanalRocketMQProducer.class);

    private DefaultMQProducer    defaultMQProducer;
    private static final String  CLOUD_ACCESS_CHANNEL = "cloud";
    protected ThreadPoolExecutor sendPartitionExecutor;

    @Override
    public void init(Properties properties) {
        RocketMQProducerConfig rocketMQProperties = new RocketMQProducerConfig();
        this.mqProperties = rocketMQProperties;
        super.init(properties);
        loadRocketMQProperties(properties);

        RPCHook rpcHook = null;
        if (mqProperties.getAliyunAccessKey().length() > 0 && mqProperties.getAliyunSecretKey().length() > 0) {
            SessionCredentials sessionCredentials = new SessionCredentials();
            sessionCredentials.setAccessKey(mqProperties.getAliyunAccessKey());
            sessionCredentials.setSecretKey(mqProperties.getAliyunSecretKey());
            rpcHook = new AclClientRPCHook(sessionCredentials);
        }

        defaultMQProducer = new DefaultMQProducer(rocketMQProperties.getProducerGroup(),
            rpcHook,
            rocketMQProperties.isEnableMessageTrace(),
            rocketMQProperties.getCustomizedTraceTopic());
        if (CLOUD_ACCESS_CHANNEL.equals(rocketMQProperties.getAccessChannel())) {
            defaultMQProducer.setAccessChannel(AccessChannel.CLOUD);
        }
        if (!StringUtils.isEmpty(rocketMQProperties.getNamespace())) {
            defaultMQProducer.setNamespace(rocketMQProperties.getNamespace());
        }
        defaultMQProducer.setNamesrvAddr(rocketMQProperties.getNamesrvAddr());
        defaultMQProducer.setRetryTimesWhenSendFailed(rocketMQProperties.getRetryTimesWhenSendFailed());
        defaultMQProducer.setVipChannelEnabled(rocketMQProperties.isVipChannelEnabled());
        logger.info("##Start RocketMQ producer##");
        try {
            defaultMQProducer.start();
        } catch (MQClientException ex) {
            throw new CanalException("Start RocketMQ producer error", ex);
        }

        int parallelPartitionSendThreadSize = mqProperties.getParallelSendThreadSize();
        sendPartitionExecutor = new ThreadPoolExecutor(parallelPartitionSendThreadSize,
            parallelPartitionSendThreadSize,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(parallelPartitionSendThreadSize * 2),
            new NamedThreadFactory("MQ-Parallel-Sender-Partition"),
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private void loadRocketMQProperties(Properties properties) {
        RocketMQProducerConfig rocketMQProperties = (RocketMQProducerConfig) this.mqProperties;
        // 兼容下<=1.1.4的mq配置
        doMoreCompatibleConvert("canal.mq.servers", "rocketmq.namesrv.addr", properties);
        doMoreCompatibleConvert("canal.mq.producerGroup", "rocketmq.producer.group", properties);
        doMoreCompatibleConvert("canal.mq.namespace", "rocketmq.namespace", properties);
        doMoreCompatibleConvert("canal.mq.retries", "rocketmq.retry.times.when.send.failed", properties);

        String producerGroup = PropertiesUtils.getProperty(properties, RocketMQConstants.ROCKETMQ_PRODUCER_GROUP);
        if (!StringUtils.isEmpty(producerGroup)) {
            rocketMQProperties.setProducerGroup(producerGroup);
        }
        String enableMessageTrace = PropertiesUtils.getProperty(properties,
            RocketMQConstants.ROCKETMQ_ENABLE_MESSAGE_TRACE);
        if (!StringUtils.isEmpty(enableMessageTrace)) {
            rocketMQProperties.setEnableMessageTrace(Boolean.parseBoolean(enableMessageTrace));
        }
        String customizedTraceTopic = PropertiesUtils.getProperty(properties,
            RocketMQConstants.ROCKETMQ_CUSTOMIZED_TRACE_TOPIC);
        if (!StringUtils.isEmpty(customizedTraceTopic)) {
            rocketMQProperties.setCustomizedTraceTopic(customizedTraceTopic);
        }
        String namespace = PropertiesUtils.getProperty(properties, RocketMQConstants.ROCKETMQ_NAMESPACE);
        if (!StringUtils.isEmpty(namespace)) {
            rocketMQProperties.setNamespace(namespace);
        }
        String namesrvAddr = PropertiesUtils.getProperty(properties, RocketMQConstants.ROCKETMQ_NAMESRV_ADDR);
        if (!StringUtils.isEmpty(namesrvAddr)) {
            rocketMQProperties.setNamesrvAddr(namesrvAddr);
        }
        String retry = PropertiesUtils.getProperty(properties, RocketMQConstants.ROCKETMQ_RETRY_TIMES_WHEN_SEND_FAILED);
        if (!StringUtils.isEmpty(retry)) {
            rocketMQProperties.setRetryTimesWhenSendFailed(Integer.parseInt(retry));
        }
        String vipChannelEnabled = PropertiesUtils.getProperty(properties,
            RocketMQConstants.ROCKETMQ_VIP_CHANNEL_ENABLED);
        if (!StringUtils.isEmpty(vipChannelEnabled)) {
            rocketMQProperties.setVipChannelEnabled(Boolean.parseBoolean(vipChannelEnabled));
        }
        String tag = PropertiesUtils.getProperty(properties, RocketMQConstants.ROCKETMQ_TAG);
        if (!StringUtils.isEmpty(tag)) {
            rocketMQProperties.setTag(tag);
        }
    }

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

    public void send(final MQDestination destination, String topicName, com.alibaba.otter.canal.protocol.Message message) {
        // 获取当前topic的分区数
        Integer partitionNum = MQMessageUtils.parseDynamicTopicPartition(topicName,
            destination.getDynamicTopicPartitionNum());
        if (partitionNum == null) {
            partitionNum = destination.getPartitionsNum();
        }
        if (!mqProperties.isFlatMessage()) {
            if (destination.getPartitionHash() != null && !destination.getPartitionHash().isEmpty()) {
                // 并发构造
                MQMessageUtils.EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
                // 串行分区
                com.alibaba.otter.canal.protocol.Message[] messages = MQMessageUtils.messagePartition(datas,
                    message.getId(),
                    partitionNum,
                    destination.getPartitionHash(),
                    mqProperties.isDatabaseHash());
                int length = messages.length;

                ExecutorTemplate template = new ExecutorTemplate(sendPartitionExecutor);
                for (int i = 0; i < length; i++) {
                    com.alibaba.otter.canal.protocol.Message dataPartition = messages[i];
                    if (dataPartition != null) {
                        final int index = i;
                        template.submit(() -> {
                            Message data = new Message(topicName,
                                ((RocketMQProducerConfig) this.mqProperties).getTag(),
                                CanalMessageSerializerUtil.serializer(dataPartition,
                                    mqProperties.isFilterTransactionEntry()));
                            sendMessage(data, index);
                        });
                    }
                }
                // 等所有分片发送完毕
                template.waitForResult();
            } else {
                final int partition = destination.getPartition() != null ? destination.getPartition() : 0;
                Message data = new Message(topicName,
                    ((RocketMQProducerConfig) this.mqProperties).getTag(),
                    CanalMessageSerializerUtil.serializer(message, mqProperties.isFilterTransactionEntry()));
                sendMessage(data, partition);
            }
        } else {
            // 并发构造
            MQMessageUtils.EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, message.getId());
            // 初始化分区合并队列
            if (destination.getPartitionHash() != null && !destination.getPartitionHash().isEmpty()) {
                List<List<FlatMessage>> partitionFlatMessages = new ArrayList<>();
                for (int i = 0; i < destination.getPartitionsNum(); i++) {
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

                ExecutorTemplate template = new ExecutorTemplate(sendPartitionExecutor);
                for (int i = 0; i < partitionFlatMessages.size(); i++) {
                    final List<FlatMessage> flatMessagePart = partitionFlatMessages.get(i);
                    if (flatMessagePart != null && flatMessagePart.size() > 0) {
                        final int index = i;
                        template.submit(() -> {
                            List<Message> messages = flatMessagePart.stream()
                                .map(flatMessage -> new Message(topicName,
                                    ((RocketMQProducerConfig) this.mqProperties).getTag(),
                                    JSON.toJSONBytes(flatMessage, SerializerFeature.WriteMapNullValue)))
                                .collect(Collectors.toList());
                            // 批量发送
                            sendMessage(messages, index);
                        });
                    }
                }

                // 批量等所有分区的结果
                template.waitForResult();
            } else {
                final int partition = destination.getPartition() != null ? destination.getPartition() : 0;
                List<Message> messages = flatMessages.stream()
                    .map(flatMessage -> new Message(topicName,
                        ((RocketMQProducerConfig) this.mqProperties).getTag(),
                        JSON.toJSONBytes(flatMessage, SerializerFeature.WriteMapNullValue)))
                    .collect(Collectors.toList());
                // 批量发送
                sendMessage(messages, partition);
            }
        }
    }

    private void sendMessage(Message message, int partition) {
        try {
            SendResult sendResult = this.defaultMQProducer.send(message, (mqs, msg, arg) -> {
                if (partition >= mqs.size()) {
                    return mqs.get(partition % mqs.size());
                } else {
                    return mqs.get(partition);
                }
            }, null);

            if (logger.isDebugEnabled()) {
                logger.debug("Send Message Result: {}", sendResult);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("deprecation")
    private void sendMessage(List<Message> messages, int partition) {
        if (messages.isEmpty()) {
            return;
        }

        // 获取一下messageQueue
        DefaultMQProducerImpl innerProducer = this.defaultMQProducer.getDefaultMQProducerImpl();
        TopicPublishInfo topicInfo = innerProducer.getTopicPublishInfoTable().get(messages.get(0).getTopic());
        if (topicInfo == null) {
            for (Message message : messages) {
                sendMessage(message, partition);
            }
        } else {
            // 批量发送
            List<MessageQueue> queues = topicInfo.getMessageQueueList();
            int size = queues.size();
            if (size <= 0) {
                // 可能是第一次创建
                for (Message message : messages) {
                    sendMessage(message, partition);
                }
            } else {
                MessageQueue queue;
                if (partition >= size) {
                    queue = queues.get(partition % size);
                } else {
                    queue = queues.get(partition);
                }

                try {
                    SendResult sendResult = this.defaultMQProducer.send(messages, queue);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Send Message Result: {}", sendResult);
                    }
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void stop() {
        logger.info("## Stop RocketMQ producer##");
        this.defaultMQProducer.shutdown();
        if (sendPartitionExecutor != null) {
            sendPartitionExecutor.shutdownNow();
        }

        super.stop();
    }
}

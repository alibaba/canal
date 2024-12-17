package com.alibaba.otter.canal.connector.kafka.producer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alibaba.otter.canal.common.utils.PropertiesUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.otter.canal.common.utils.ExecutorTemplate;
import com.alibaba.otter.canal.connector.core.producer.AbstractMQProducer;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.producer.MQMessageUtils;
import com.alibaba.otter.canal.connector.core.producer.MQMessageUtils.EntryRowData;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.connector.kafka.config.KafkaConstants;
import com.alibaba.otter.canal.connector.kafka.config.KafkaProducerConfig;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * kafka producer SPI 实现
 *
 * @author rewerma 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
@SPI("kafka")
public class CanalKafkaProducer extends AbstractMQProducer implements CanalMQProducer {

    private static final Logger      logger              = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private static final String      PREFIX_KAFKA_CONFIG = "kafka.";

    private Producer<String, byte[]> producer;

    @Override
    public void init(Properties properties) {
        KafkaProducerConfig kafkaProducerConfig = new KafkaProducerConfig();
        this.mqProperties = kafkaProducerConfig;
        super.init(properties);
        // load properties
        this.loadKafkaProperties(properties);

        Properties kafkaProperties = new Properties();
        kafkaProperties.putAll(kafkaProducerConfig.getKafkaProperties());
        kafkaProperties.put("max.in.flight.requests.per.connection", 1);
        kafkaProperties.put("key.serializer", StringSerializer.class);
        if (kafkaProducerConfig.isKerberosEnabled()) {
            File krb5File = new File(kafkaProducerConfig.getKrb5File());
            File jaasFile = new File(kafkaProducerConfig.getJaasFile());
            if (krb5File.exists() && jaasFile.exists()) {
                // 配置kerberos认证，需要使用绝对路径
                System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
                System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                kafkaProperties.put("security.protocol", "SASL_PLAINTEXT");
                kafkaProperties.put("sasl.kerberos.service.name", "kafka");
            } else {
                String errorMsg = "ERROR # The kafka kerberos configuration file does not exist! please check it";
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
        kafkaProperties.put("value.serializer", KafkaMessageSerializer.class);
        producer = new KafkaProducer<>(kafkaProperties);
    }

    private void loadKafkaProperties(Properties properties) {
        KafkaProducerConfig kafkaProducerConfig = (KafkaProducerConfig) this.mqProperties;
        Map<String, Object> kafkaProperties = kafkaProducerConfig.getKafkaProperties();
        // 兼容下<=1.1.4的mq配置
        doMoreCompatibleConvert("canal.mq.servers", "kafka.bootstrap.servers", properties);
        doMoreCompatibleConvert("canal.mq.acks", "kafka.acks", properties);
        doMoreCompatibleConvert("canal.mq.compressionType", "kafka.compression.type", properties);
        doMoreCompatibleConvert("canal.mq.retries", "kafka.retries", properties);
        doMoreCompatibleConvert("canal.mq.batchSize", "kafka.batch.size", properties);
        doMoreCompatibleConvert("canal.mq.lingerMs", "kafka.linger.ms", properties);
        doMoreCompatibleConvert("canal.mq.maxRequestSize", "kafka.max.request.size", properties);
        doMoreCompatibleConvert("canal.mq.bufferMemory", "kafka.buffer.memory", properties);
        doMoreCompatibleConvert("canal.mq.kafka.kerberos.enable", "kafka.kerberos.enable", properties);
        doMoreCompatibleConvert("canal.mq.kafka.kerberos.krb5.file", "kafka.kerberos.krb5.file", properties);
        doMoreCompatibleConvert("canal.mq.kafka.kerberos.jaas.file", "kafka.kerberos.jaas.file", properties);

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            Object value = entry.getValue();
            if (key.startsWith(PREFIX_KAFKA_CONFIG) && value != null) {
                // check env config
                value = PropertiesUtils.getProperty(properties, key);
                key = key.substring(PREFIX_KAFKA_CONFIG.length());
                kafkaProperties.put(key, value);
            }
        }
        String kerberosEnabled = PropertiesUtils.getProperty(properties, KafkaConstants.CANAL_MQ_KAFKA_KERBEROS_ENABLE);
        if (!StringUtils.isEmpty(kerberosEnabled)) {
            kafkaProducerConfig.setKerberosEnabled(Boolean.parseBoolean(kerberosEnabled));
        }
        String krb5File = PropertiesUtils.getProperty(properties, KafkaConstants.CANAL_MQ_KAFKA_KERBEROS_KRB5_FILE);
        if (!StringUtils.isEmpty(krb5File)) {
            kafkaProducerConfig.setKrb5File(krb5File);
        }
        String jaasFile = PropertiesUtils.getProperty(properties, KafkaConstants.CANAL_MQ_KAFKA_KERBEROS_JAAS_FILE);
        if (!StringUtils.isEmpty(jaasFile)) {
            kafkaProducerConfig.setJaasFile(jaasFile);
        }
    }

    @Override
    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            if (producer != null) {
                producer.close();
            }
            super.stop();
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    @Override
    public void send(MQDestination mqDestination, Message message, Callback callback) {
        ExecutorTemplate template = new ExecutorTemplate(sendExecutor);

        try {
            List result;
            logger.info("self dynamicTopic = " + mqDestination.getDynamicTopic().substring(5));

            if (!StringUtils.isEmpty(mqDestination.getDynamicTopic())) {
                if (mqDestination.getDynamicTopic().startsWith("self|")) {
                    logger.info("self dynamicTopic = " + mqDestination.getDynamicTopic().substring(5));
                    System.out.println();
                    // 如果dynamicTopic以self打头，走自定义规则
                    Map<Integer, Message> messageMap = messageTopicsForPartition(message, mqDestination.getTopic(), mqDestination.getDynamicTopic());

                    // 针对不同的topic,引入多线程提升效率
                    for (Map.Entry<Integer, Message> entry : messageMap.entrySet()) {
                        final Message messageSub = entry.getValue();
                        template.submit((Callable) () -> {
                            try {
                                return sendForCustom(mqDestination.getTopic(), messageSub, mqProperties.isFlatMessage(), entry.getKey());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

                    result = template.waitForResult();
                } else {
                    // 动态topic路由计算,只是基于schema/table,不涉及proto数据反序列化
                    Map<String, Message> messageMap = MQMessageUtils.messageTopics(message, mqDestination.getTopic(), mqDestination.getDynamicTopic());

                    // 针对不同的topic,引入多线程提升效率
                    for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
                        final String topicName = entry.getKey().replace('.', '_');
                        final Message messageSub = entry.getValue();
                        template.submit((Callable) () -> {
                            try {
                                return send(mqDestination, topicName, messageSub, mqProperties.isFlatMessage());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

                    result = template.waitForResult();
                }

            } else {
                result = new ArrayList();
                List<Future> futures = send(mqDestination,
                    mqDestination.getTopic(),
                    message,
                    mqProperties.isFlatMessage());
                result.add(futures);
            }

            // 一个批次的所有topic和分区的队列，都采用异步的模式进行多线程批量发送
            // 最后在集结点进行flush等待，确保所有数据都写出成功
            // 注意：kafka的异步模式如果要保证顺序性，需要设置max.in.flight.requests.per.connection=1，确保在网络异常重试时有排他性
            producer.flush();
            // flush操作也有可能是发送失败,这里需要异步关注一下发送结果,针对有异常的直接出发rollback
            for (Object obj : result) {
                List<Future> futures = (List<Future>) obj;
                for (Future future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            callback.commit();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            callback.rollback();
        } finally {
            template.clear();
        }
    }

    private List<Future> sendForCustom(String topicName, Message message, boolean flat, int partition) {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        if (!flat) {
            records.add(new ProducerRecord<>(topicName, partition, null, CanalMessageSerializerUtil.serializer(message, mqProperties.isFilterTransactionEntry())));
        } else {
            // 发送扁平数据json
            // 并发构造
            EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, message.getId());
            for (FlatMessage flatMessage : flatMessages) {
                records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(flatMessage, JSONWriter.Feature.WriteNulls)));
            }
        }

        return produce(records);
    }

    private List<Future> send(MQDestination mqDestination, String topicName, Message message, boolean flat) {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        // 获取当前topic的分区数
        Integer partitionNum = MQMessageUtils.parseDynamicTopicPartition(topicName, mqDestination.getDynamicTopicPartitionNum());
        if (partitionNum == null) {
            partitionNum = mqDestination.getPartitionsNum();
        }
        if (!flat) {
            if (mqDestination.getPartitionHash() != null && !mqDestination.getPartitionHash().isEmpty()) {
                // 并发构造
                EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
                // 串行分区
                Message[] messages = MQMessageUtils.messagePartition(datas,
                    message.getId(),
                    partitionNum,
                    mqDestination.getPartitionHash(),
                    this.mqProperties.isDatabaseHash());
                int length = messages.length;
                for (int i = 0; i < length; i++) {
                    Message messagePartition = messages[i];
                    if (messagePartition != null) {
                        records.add(new ProducerRecord<>(topicName,
                            i,
                            null,
                            CanalMessageSerializerUtil.serializer(messagePartition,
                                mqProperties.isFilterTransactionEntry())));
                    }
                }
            } else {
                final int partition = mqDestination.getPartition() != null ? mqDestination.getPartition() : 0;
                records.add(new ProducerRecord<>(topicName,
                    partition,
                    null,
                    CanalMessageSerializerUtil.serializer(message, mqProperties.isFilterTransactionEntry())));
            }
        } else {
            // 发送扁平数据json
            // 并发构造
            EntryRowData[] datas = MQMessageUtils.buildMessageData(message, buildExecutor);
            // 串行分区
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(datas, message.getId());
            for (FlatMessage flatMessage : flatMessages) {
                if (mqDestination.getPartitionHash() != null && !mqDestination.getPartitionHash().isEmpty()) {
                    FlatMessage[] partitionFlatMessage = MQMessageUtils.messagePartition(flatMessage,
                        partitionNum,
                        mqDestination.getPartitionHash(),
                        this.mqProperties.isDatabaseHash());
                    int length = partitionFlatMessage.length;
                    for (int i = 0; i < length; i++) {
                        FlatMessage flatMessagePart = partitionFlatMessage[i];
                        if (flatMessagePart != null) {
                            records.add(new ProducerRecord<>(topicName, i, null, JSON.toJSONBytes(flatMessagePart,
                                JSONWriter.Feature.WriteNulls)));
                        }
                    }
                } else {
                    final int partition = mqDestination.getPartition() != null ? mqDestination.getPartition() : 0;
                    records.add(new ProducerRecord<>(topicName, partition, null, JSON.toJSONBytes(flatMessage,
                        JSONWriter.Feature.WriteNulls)));
                }
            }
        }

        return produce(records);
    }

    private List<Future> produce(List<ProducerRecord<String, byte[]>> records) {
        List<Future> futures = new ArrayList<>();
        // 异步发送，因为在partition hash的时候已经按照每个分区合并了消息，走到这一步不需要考虑单个分区内的顺序问题
        for (ProducerRecord record : records) {
            futures.add(producer.send(record));
        }

        return futures;
    }

    /**
     * 自定义分配规则 按 schema 或者 schema+table 将 message 分配到对应topic
     *
     * @param message 原message
     * @param defaultTopic 默认topic
     * @param dynamicTopicConfigs 动态topic规则
     * @return 分隔后的message map
     */
    public static Map<Integer, Message> messageTopicsForPartition(Message message, String defaultTopic, String dynamicTopicConfigs) {
        dynamicTopicConfigs = dynamicTopicConfigs.substring("self|".length());
        List<CanalEntry.Entry> entries;
        if (message.isRaw()) {
            List<ByteString> rawEntries = message.getRawEntries();
            entries = new ArrayList<>(rawEntries.size());
            for (ByteString byteString : rawEntries) {
                CanalEntry.Entry entry;
                try {
                    entry = CanalEntry.Entry.parseFrom(byteString);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                entries.add(entry);
            }
        } else {
            entries = message.getEntries();
        }
        Map<Integer, Message> messages = new HashMap<>();
        Map<String, Integer> partitions = new HashMap<>();
        String[] configs = dynamicTopicConfigs.split(",");
        if (configs != null) {
            for (String config : configs) {
                String[] tablePartition = config.split(":");
                partitions.put(tablePartition[0], Integer.valueOf(tablePartition[1]));
            }
        }

        for (CanalEntry.Entry entry : entries) {
            // 如果有topic路由,则忽略begin/end事件
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

            logger.info("schemaName and tableName  {} {}",schemaName,tableName);

            if (StringUtils.isEmpty(schemaName) || StringUtils.isEmpty(tableName)) {
                logger.info("schemaName or tableName is null {} {} skip",schemaName,tableName);
//                System.out.println(entry.getHeader());
//                put2MapMessageForPartition(messages, message.getId(), defaultTopic, entry);
            } else {

                if (partitions.containsKey(schemaName + "." + tableName)) {
                    Integer partitionId = partitions.get(schemaName + "." + tableName);
                    Message msg = messages.get(partitionId);
                    if (msg == null) {
                        msg = new Message(message.getId(), new ArrayList<>());
                        messages.put(partitionId, msg);
                    }
                    msg.getEntries().add(entry);

                }

            }
        }
        return messages;
    }


}

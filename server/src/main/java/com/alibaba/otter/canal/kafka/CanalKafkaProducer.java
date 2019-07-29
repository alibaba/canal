package com.alibaba.otter.canal.kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.MQMessageUtils;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spi.CanalMQProducer;

/**
 * kafka producer 主操作类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaProducer implements CanalMQProducer {

    private static final Logger       logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private Producer<String, Message> producer;
    private Producer<String, String>  producer2;                                                 // 用于扁平message的数据投递
    private MQProperties              kafkaProperties;

    @Override
    public void init(MQProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaProperties.getServers());
        properties.put("acks", kafkaProperties.getAcks());
        properties.put("compression.type", kafkaProperties.getCompressionType());
        properties.put("batch.size", kafkaProperties.getBatchSize());
        properties.put("linger.ms", kafkaProperties.getLingerMs());
        properties.put("max.request.size", kafkaProperties.getMaxRequestSize());
        properties.put("buffer.memory", kafkaProperties.getBufferMemory());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("max.in.flight.requests.per.connection", 1);

        if (!kafkaProperties.getProperties().isEmpty()) {
            properties.putAll(kafkaProperties.getProperties());
        }
        properties.put("retries", kafkaProperties.getRetries());
        if (kafkaProperties.isKerberosEnable()) {
            File krb5File = new File(kafkaProperties.getKerberosKrb5FilePath());
            File jaasFile = new File(kafkaProperties.getKerberosJaasFilePath());
            if (krb5File.exists() && jaasFile.exists()) {
                // 配置kerberos认证，需要使用绝对路径
                System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
                System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
                System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.kerberos.service.name", "kafka");
            } else {
                String errorMsg = "ERROR # The kafka kerberos configuration file does not exist! please check it";
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }

        if (!kafkaProperties.getFlatMessage()) {
            properties.put("value.serializer", MessageSerializer.class.getName());
            producer = new KafkaProducer<String, Message>(properties);
        } else {
            properties.put("value.serializer", StringSerializer.class.getName());
            producer2 = new KafkaProducer<String, String>(properties);
        }
    }

    @Override
    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            if (producer != null) {
                producer.close();
            }
            if (producer2 != null) {
                producer2.close();
            }
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    @Override
    public void send(MQProperties.CanalDestination canalDestination, Message message, Callback callback) {
        try {
            if (!StringUtils.isEmpty(canalDestination.getDynamicTopic())) {
                // 动态topic
                Map<String, Message> messageMap = MQMessageUtils.messageTopics(message,
                    canalDestination.getTopic(),
                    canalDestination.getDynamicTopic());

                for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
                    String topicName = entry.getKey(); //.replace('.', '_');
                    Message messageSub = entry.getValue();
                    if (logger.isDebugEnabled()) {
                        logger.debug("## Send message to kafka topic: " + topicName);
                    }
                    send(canalDestination, topicName, messageSub);
                }
            } else {
                send(canalDestination, canalDestination.getTopic(), message);
            }
            callback.commit();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            callback.rollback();
        }
    }

    private void send(MQProperties.CanalDestination canalDestination, String topicName, Message message)
                                                                                                        throws Exception {
        if (!kafkaProperties.getFlatMessage()) {
            List<ProducerRecord> records = new ArrayList<ProducerRecord>();
            if (canalDestination.getPartitionHash() != null && !canalDestination.getPartitionHash().isEmpty()) {
                Message[] messages = MQMessageUtils.messagePartition(message,
                    canalDestination.getPartitionsNum(),
                    canalDestination.getPartitionHash());
                int length = messages.length;
                for (int i = 0; i < length; i++) {
                    Message messagePartition = messages[i];
                    if (messagePartition != null) {
                        records.add(new ProducerRecord<String, Message>(topicName, i, null, messagePartition));
                    }
                }
            } else {
                final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
                records.add(new ProducerRecord<String, Message>(topicName, partition, null, message));
            }

            produce(topicName, records, false);
        } else {
            // 发送扁平数据json
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(message);
            List<ProducerRecord> records = new ArrayList<ProducerRecord>();
            if (flatMessages != null) {
                for (FlatMessage flatMessage : flatMessages) {
                    if (canalDestination.getPartitionHash() != null && !canalDestination.getPartitionHash().isEmpty()) {
                        FlatMessage[] partitionFlatMessage = MQMessageUtils.messagePartition(flatMessage,
                            canalDestination.getPartitionsNum(),
                            canalDestination.getPartitionHash());
                        int length = partitionFlatMessage.length;
                        for (int i = 0; i < length; i++) {
                            FlatMessage flatMessagePart = partitionFlatMessage[i];
                            if (flatMessagePart != null) {
                                records.add(new ProducerRecord<String, String>(topicName,
                                    i,
                                    null,
                                    JSON.toJSONString(flatMessagePart, SerializerFeature.WriteMapNullValue)));
                            }
                        }
                    } else {
                        final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
                        records.add(new ProducerRecord<String, String>(topicName,
                            partition,
                            null,
                            JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue)));
                    }

                    // 每条记录需要flush
                    produce(topicName, records, true);
                }
            }
        }
    }

    private void produce(String topicName, List<ProducerRecord> records, boolean flatMessage) {

        Producer producerTmp = null;
        if (flatMessage) {
            producerTmp = producer2;
        } else {
            producerTmp = producer;
        }

        List<Future> futures = new ArrayList<Future>();
        try {
            // 异步发送，因为在partition hash的时候已经按照每个分区合并了消息，走到这一步不需要考虑单个分区内的顺序问题
            for (ProducerRecord record : records) {
                futures.add(producerTmp.send(record));
            }
        } finally {
            if (logger.isDebugEnabled()) {
                for (ProducerRecord record : records) {
                    logger.debug("Send  message to kafka topic: [{}], packet: {}", topicName, record.toString());
                }
            }
            // 批量刷出
            producerTmp.flush();

            // flush操作也有可能是发送失败,这里需要异步关注一下发送结果,针对有异常的直接出发rollback
            for (Future future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}

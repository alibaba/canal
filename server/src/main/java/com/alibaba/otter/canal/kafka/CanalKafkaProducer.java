package com.alibaba.otter.canal.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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

        if (kafkaProperties.getTransaction()) {
            properties.put("transactional.id", "canal-transactional-id");
        } else {
            properties.put("retries", kafkaProperties.getRetries());
        }
        if (!kafkaProperties.getFlatMessage()) {
            properties.put("value.serializer", MessageSerializer.class.getName());
            producer = new KafkaProducer<String, Message>(properties);
        } else {
            properties.put("value.serializer", StringSerializer.class.getName());
            producer2 = new KafkaProducer<String, String>(properties);
        }
        if (kafkaProperties.getTransaction()) {
            if (!kafkaProperties.getFlatMessage()) {
                producer.initTransactions();
            } else {
                producer2.initTransactions();
            }
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
        // 开启事务，需要kafka版本支持
        Producer producerTmp;
        if (!kafkaProperties.getFlatMessage()) {
            producerTmp = producer;
        } else {
            producerTmp = producer2;
        }

        if (kafkaProperties.getTransaction()) {
            producerTmp.beginTransaction();
        }
        try {
            if (!StringUtils.isEmpty(canalDestination.getDynamicTopic())) {
                // 动态topic
                Map<String, Message> messageMap = MQMessageUtils.messageTopics(message,
                    canalDestination.getTopic(),
                    canalDestination.getDynamicTopic());

                for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
                    String topicName = entry.getKey().replace('.', '_');
                    Message messageSub = entry.getValue();
                    if (logger.isDebugEnabled()) {
                        logger.debug("## Send message to kafka topic: " + topicName);
                    }
                    send(canalDestination, topicName, messageSub);
                }
            } else {
                send(canalDestination, canalDestination.getTopic(), message);
            }
            if (kafkaProperties.getTransaction()) {
                producerTmp.commitTransaction();
            }
            callback.commit();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            if (kafkaProperties.getTransaction()) {
                producerTmp.abortTransaction();
            }
            callback.rollback();
        }
    }

    private void send(MQProperties.CanalDestination canalDestination, String topicName, Message message)
                                                                                                        throws Exception {
        if (!kafkaProperties.getFlatMessage()) {
            List<ProducerRecord<String, Message>> records = new ArrayList<ProducerRecord<String, Message>>();
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

            if (!records.isEmpty()) {
                for (ProducerRecord<String, Message> record : records) {
                    producer.send(record).get();
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Send  message to kafka topic: [{}], packet: {}", topicName, message.toString());
                }
            }
        } else {
            // 发送扁平数据json
            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(message);
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
                                produce(topicName, i, flatMessagePart);
                            }
                        }
                    } else {
                        final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
                        produce(topicName, partition, flatMessage);
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Send flat message to kafka topic: [{}], packet: {}",
                            topicName,
                            JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue));
                    }
                }
            }
        }
    }

    private void produce(String topicName, int partition, FlatMessage flatMessage) throws ExecutionException,
                                                                                  InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,
            partition,
            null,
            JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue));
        if (kafkaProperties.getTransaction()) {
            producer2.send(record);
        } else {
            producer2.send(record);
        }
    }

}

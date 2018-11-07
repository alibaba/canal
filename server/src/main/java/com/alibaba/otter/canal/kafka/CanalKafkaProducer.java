package com.alibaba.otter.canal.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spi.CanalMQProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

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
        properties.put("acks", "all");
        properties.put("retries", kafkaProperties.getRetries());
        properties.put("batch.size", kafkaProperties.getBatchSize());
        properties.put("linger.ms", kafkaProperties.getLingerMs());
        properties.put("buffer.memory", kafkaProperties.getBufferMemory());
        properties.put("key.serializer", StringSerializer.class.getName());
        if (!kafkaProperties.getFlatMessage()) {
            properties.put("value.serializer", MessageSerializer.class.getName());
            producer = new KafkaProducer<String, Message>(properties);
        } else {
            properties.put("value.serializer", StringSerializer.class.getName());
            producer2 = new KafkaProducer<String, String>(properties);
        }

        // producer.initTransactions();
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

        // producer.beginTransaction();
        if (!kafkaProperties.getFlatMessage()) {
            try {
                ProducerRecord<String, Message> record;
                if (canalDestination.getPartition() != null) {
                    record = new ProducerRecord<String, Message>(canalDestination.getTopic(),
                        canalDestination.getPartition(),
                        null,
                        message);
                } else {
                    record = new ProducerRecord<String, Message>(canalDestination.getTopic(), 0, null, message);
                }

                producer.send(record).get();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                // producer.abortTransaction();
                callback.rollback();
                return;
            }
        } else {
            // 发送扁平数据json
            List<FlatMessage> flatMessages = FlatMessage.messageConverter(message);
            if (flatMessages != null) {
                for (FlatMessage flatMessage : flatMessages) {
                    if (canalDestination.getPartition() != null) {
                        try {
                            ProducerRecord<String, String> record = new ProducerRecord<String, String>(canalDestination.getTopic(),
                                canalDestination.getPartition(),
                                null,
                                JSON.toJSONString(flatMessage));
                            producer2.send(record).get();
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            // producer.abortTransaction();
                            callback.rollback();
                            return;
                        }
                    } else {
                        if (canalDestination.getPartitionHash() != null
                            && !canalDestination.getPartitionHash().isEmpty()) {
                            FlatMessage[] partitionFlatMessage = FlatMessage.messagePartition(flatMessage,
                                canalDestination.getPartitionsNum(),
                                canalDestination.getPartitionHash());
                            int length = partitionFlatMessage.length;
                            for (int i = 0; i < length; i++) {
                                FlatMessage flatMessagePart = partitionFlatMessage[i];
                                if (flatMessagePart != null) {
                                    try {
                                        ProducerRecord<String, String> record = new ProducerRecord<String, String>(canalDestination.getTopic(),
                                            i,
                                            null,
                                            JSON.toJSONString(flatMessagePart));
                                        producer2.send(record).get();
                                    } catch (Exception e) {
                                        logger.error(e.getMessage(), e);
                                        // producer.abortTransaction();
                                        callback.rollback();
                                        return;
                                    }
                                }
                            }
                        } else {
                            try {
                                ProducerRecord<String, String> record = new ProducerRecord<String, String>(canalDestination.getTopic(),
                                    0,
                                    null,
                                    JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue));
                                producer2.send(record).get();
                            } catch (Exception e) {
                                logger.error(e.getMessage(), e);
                                // producer.abortTransaction();
                                callback.rollback();
                                return;
                            }
                        }
                    }
                }
            }
        }

        // producer.commitTransaction();
        callback.commit();
        if (logger.isDebugEnabled()) {
            logger.debug("send message to kafka topic: {}", canalDestination.getTopic());
        }

    }

}

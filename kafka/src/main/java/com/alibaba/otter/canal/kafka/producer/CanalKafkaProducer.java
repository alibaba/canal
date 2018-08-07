package com.alibaba.otter.canal.kafka.producer;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.kafka.producer.KafkaProperties.Topic;
import com.alibaba.otter.canal.protocol.Message;

/**
 * kafka producer 主操作类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaProducer {

    private static final Logger       logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private Producer<String, Message> producer;

    public void init(KafkaProperties kafkaProperties) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaProperties.getServers());
        properties.put("acks", "all");
        properties.put("retries", kafkaProperties.getRetries());
        properties.put("batch.size", kafkaProperties.getBatchSize());
        properties.put("linger.ms", kafkaProperties.getLingerMs());
        properties.put("buffer.memory", kafkaProperties.getBufferMemory());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", MessageSerializer.class.getName());
        producer = new KafkaProducer<String, Message>(properties);
    }

    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            producer.close();
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    public void send(Topic topic, Message message) throws IOException {
        // set canal.instance.filter.transaction.entry = true

        // boolean valid = false;
        // if (message != null) {
        // if (message.isRaw() && !message.getRawEntries().isEmpty()) {
        // for (ByteString byteString : message.getRawEntries()) {
        // CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(byteString);
        // if (entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONBEGIN
        // && entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONEND) {
        // valid = true;
        // break;
        // }
        // }
        // } else if (!message.getEntries().isEmpty()){
        // for (CanalEntry.Entry entry : message.getEntries()) {
        // if (entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONBEGIN
        // && entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONEND) {
        // valid = true;
        // break;
        // }
        // }
        // }
        // }
        // if (!valid) {
        // return;
        // }
        ProducerRecord<String, Message> record;
        if (topic.getPartition() != null) {
            record = new ProducerRecord<String, Message>(topic.getTopic(), topic.getPartition(), null, message);
        } else {
            record = new ProducerRecord<String, Message>(topic.getTopic(), message);
        }
        producer.send(record);
        logger.debug("send message to kafka topic: {} \n {}", topic.getTopic(), message.toString());
    }
}

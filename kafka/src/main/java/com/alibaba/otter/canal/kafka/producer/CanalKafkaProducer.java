package com.alibaba.otter.canal.kafka.producer;

import com.alibaba.otter.canal.kafka.producer.KafkaProperties.Topic;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
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
public class CanalKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private static Producer<String, Message> producer;

    static void init(KafkaProperties kafkaProperties) {
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

    static void stop() {
        try {
            logger.info("## stop the kafka producer");
            producer.close();
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    static void send(Topic topic, Message message) {
        try {
            List<CanalEntry.Entry> entries = message.getEntries();
            boolean flag = false;
            if (!entries.isEmpty()) {
                for (CanalEntry.Entry entry : entries) {
                    CanalEntry.RowChange rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    if (rowChage.getIsDdl()) {
                        flag = true;
                        break;
                    } else {
                        if (!rowChage.getRowDatasList().isEmpty()) {
                            flag = true;
                            break;
                        }
                    }
                }
            }
            if (!flag) {
                return;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return;
        }

        ProducerRecord<String, Message> record;
        if (topic.getPartition() != null) {
            record = new ProducerRecord<String, Message>(topic.getTopic(), topic.getPartition(), null, message);
        } else {
            record = new ProducerRecord<String, Message>(topic.getTopic(), message);
        }
        producer.send(record);
    }
}

package com.alibaba.otter.canal.connector.kafka.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.connector.core.config.CanalConstants;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.spi.CanalMsgConsumer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.MessageUtil;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Kafka consumer SPI 实现
 *
 * @author rewerma @ 2020-02-01
 * @version 1.0.0
 */
@SPI("kafka")
public class CanalKafkaConsumer implements CanalMsgConsumer {

    private static final String      PREFIX_KAFKA_CONFIG = "kafka.";

    private KafkaConsumer<String, ?> kafkaConsumer;
    private boolean                  flatMessage         = true;
    private String                   topic;

    private Map<Integer, Long>       currentOffsets      = new ConcurrentHashMap<>();
    private Properties               kafkaProperties     = new Properties();

    @Override
    public void init(Properties properties, String topic, String groupId) {
        this.topic = topic;

        Boolean flatMessage = (Boolean) properties.get(CanalConstants.CANAL_MQ_FLAT_MESSAGE);
        if (flatMessage != null) {
            this.flatMessage = flatMessage;
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String k = (String) entry.getKey();
            Object v = entry.getValue();
            if (k.startsWith(PREFIX_KAFKA_CONFIG) && v != null) {
                kafkaProperties.put(k.substring(PREFIX_KAFKA_CONFIG.length()), v);
            }
        }
        kafkaProperties.put("group.id", groupId);
        kafkaProperties.put("key.deserializer", StringDeserializer.class);
        kafkaProperties.put("client.id", UUID.randomUUID().toString().substring(0, 6));
    }

    @Override
    public void connect() {
        if (this.flatMessage) {
            kafkaProperties.put("value.deserializer", StringDeserializer.class);
            this.kafkaConsumer = new KafkaConsumer<String, String>(kafkaProperties);
        } else {
            kafkaProperties.put("value.deserializer", KafkaMessageDeserializer.class);
            this.kafkaConsumer = new KafkaConsumer<String, Message>(kafkaProperties);
        }
        kafkaConsumer.subscribe(Collections.singletonList(topic));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<CommonMessage> getMessage(Long timeout, TimeUnit unit) {
        if (!flatMessage) {
            ConsumerRecords<String, Message> records = (ConsumerRecords<String, Message>) kafkaConsumer.poll(unit.toMillis(timeout));
            if (!records.isEmpty()) {
                currentOffsets.clear();
                List<CommonMessage> messages = new ArrayList<>();
                for (ConsumerRecord<String, Message> record : records) {
                    if (currentOffsets.get(record.partition()) == null) {
                        currentOffsets.put(record.partition(), record.offset());
                    }
                    messages.addAll(MessageUtil.convert(record.value()));
                }
                return messages;
            }
        } else {
            ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) kafkaConsumer.poll(unit.toMillis(timeout));

            if (!records.isEmpty()) {
                List<CommonMessage> messages = new ArrayList<>();
                currentOffsets.clear();
                for (ConsumerRecord<String, String> record : records) {
                    if (currentOffsets.get(record.partition()) == null) {
                        currentOffsets.put(record.partition(), record.offset());
                    }
                    String flatMessageJson = record.value();
                    CommonMessage flatMessages = JSON.parseObject(flatMessageJson, CommonMessage.class);
                    messages.add(flatMessages);
                }
                return messages;
            }
        }
        return null;
    }

    @Override
    public void rollback() {
        // 回滚所有分区
        if (kafkaConsumer != null) {
            for (Map.Entry<Integer, Long> entry : currentOffsets.entrySet()) {
                kafkaConsumer.seek(new TopicPartition(topic, entry.getKey()), currentOffsets.get(entry.getKey()));
                kafkaConsumer.commitSync();
            }
        }
    }

    @Override
    public void ack() {
        if (kafkaConsumer != null) {
            kafkaConsumer.commitSync();
        }
    }

    @Override
    public void disconnect() {
        if (kafkaConsumer != null) {
            kafkaConsumer.unsubscribe();
        }
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
            kafkaConsumer = null;
        }
    }
}

package com.alibaba.otter.canal.client.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.kafka.protocol.KafkaFlatMessage;
import com.alibaba.otter.canal.client.kafka.protocol.KafkaMessage;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * kafka带消息offset的连接器
 *
 * @Author panjianping
 * @Email ipanjianping@qq.com
 * @Date 2018/12/17
 */
public class KafkaOffsetCanalConnector extends KafkaCanalConnector {

    public KafkaOffsetCanalConnector(String servers, String topic, Integer partition, String groupId, boolean flatMessage) {
        super(servers, topic, partition, groupId, 100, flatMessage);
        // 启动时从未消费的消息位置开始
        properties.put("auto.offset.reset", "earliest");
    }

    /**
     * 获取Kafka消息，不确认
     *
     * @param timeout
     * @param unit
     * @param offset  消息偏移地址（-1为不偏移）
     * @return
     * @throws CanalClientException
     */
    public List<KafkaMessage> getListWithoutAck(Long timeout, TimeUnit unit, long offset) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return Lists.newArrayList();
        }

        if (offset > -1) {
            TopicPartition tp = new TopicPartition(topic, partition == null ? 0 : partition);
            kafkaConsumer.seek(tp, offset);
        }

        ConsumerRecords<String, Message> records = kafkaConsumer.poll(unit.toMillis(timeout));

        if (!records.isEmpty()) {
            List<KafkaMessage> messages = new ArrayList<>();
            for (ConsumerRecord<String, Message> record : records) {
                KafkaMessage message = new KafkaMessage(record.value(), record.offset());
                messages.add(message);
            }
            return messages;
        }
        return Lists.newArrayList();
    }

    /**
     * 获取Kafka消息，不确认
     *
     * @param timeout
     * @param unit
     * @param offset  消息偏移地址（-1为不偏移）
     * @return
     * @throws CanalClientException
     */
    public List<KafkaFlatMessage> getFlatListWithoutAck(Long timeout, TimeUnit unit, long offset) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return Lists.newArrayList();
        }

        if (offset > -1) {
            TopicPartition tp = new TopicPartition(topic, partition == null ? 0 : partition);
            kafkaConsumer2.seek(tp, offset);
        }

        ConsumerRecords<String, String> records = kafkaConsumer2.poll(unit.toMillis(timeout));
        if (!records.isEmpty()) {
            List<KafkaFlatMessage> flatMessages = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                String flatMessageJson = record.value();
                FlatMessage flatMessage = JSON.parseObject(flatMessageJson, FlatMessage.class);
                KafkaFlatMessage message = new KafkaFlatMessage(flatMessage, record.offset());
                flatMessages.add(message);
            }

            return flatMessages;
        }
        return Lists.newArrayList();
    }

    /**
     * 重新设置AutoOffsetReset（默认 earliest ）
     *
     * @param value
     */
    public void setAutoOffsetReset(String value) {
        if (StringUtils.isNotBlank(value)) {
            properties.put("auto.offset.reset", value);
        }
    }
}

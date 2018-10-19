package com.alibaba.otter.canal.client.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * canal kafka 数据操作客户端
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class KafkaCanalConnector {

    private KafkaConsumer<String, Message> kafkaConsumer;
    private KafkaConsumer<String, String>  kafkaConsumer2;   // 用于扁平message的数据消费
    private String                         topic;
    private Integer                        partition;
    private Properties                     properties;
    private volatile boolean               connected = false;
    private volatile boolean               running   = false;
    private boolean                        flatMessage;

    public KafkaCanalConnector(String servers, String topic, Integer partition, String groupId, boolean flatMessage){
        this.topic = topic;
        this.partition = partition;
        this.flatMessage = flatMessage;

        properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", false);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest"); // 如果没有offset则从最后的offset开始读
        properties.put("request.timeout.ms", "40000"); // 必须大于session.timeout.ms的设置
        properties.put("session.timeout.ms", "30000"); // 默认为30秒
        properties.put("max.poll.records", "100");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        if (!flatMessage) {
            properties.put("value.deserializer", MessageDeserializer.class.getName());
        } else {
            properties.put("value.deserializer", StringDeserializer.class.getName());
        }
    }

    /**
     * 重新设置sessionTime
     *
     * @param timeout
     * @param unit
     */
    public void setSessionTimeout(Long timeout, TimeUnit unit) {
        long t = unit.toMillis(timeout);
        properties.put("request.timeout.ms", String.valueOf(t + 60000));
        properties.put("session.timeout.ms", String.valueOf(t));
    }

    /**
     * 打开连接
     */
    public void connect() {
        if (connected) {
            return;
        }

        connected = true;

        if (kafkaConsumer == null && !flatMessage) {
            kafkaConsumer = new KafkaConsumer<String, Message>(properties);
        }
        if (kafkaConsumer2 == null && flatMessage) {
            kafkaConsumer2 = new KafkaConsumer<String, String>(properties);
        }
    }

    /**
     * 关闭链接
     */
    public void disconnect() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
        if (kafkaConsumer2 != null) {
            kafkaConsumer2.close();
        }

        connected = false;
    }

    private void waitClientRunning() {
        running = true;
    }

    public boolean checkValid() {
        return true;// 默认都放过
    }

    /**
     * 订阅topic
     */
    public void subscribe() {
        waitClientRunning();
        if (!running) {
            return;
        }

        if (partition == null) {
            if (kafkaConsumer != null) {
                kafkaConsumer.subscribe(Collections.singletonList(topic));
            }
            if (kafkaConsumer2 != null) {
                kafkaConsumer2.subscribe(Collections.singletonList(topic));
            }
        } else {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            if (kafkaConsumer != null) {
                kafkaConsumer.assign(Collections.singletonList(topicPartition));
            }
            if (kafkaConsumer2 != null) {
                kafkaConsumer2.assign(Collections.singletonList(topicPartition));
            }
        }
    }

    /**
     * 取消订阅
     */
    public void unsubscribe() {
        waitClientRunning();
        if (!running) {
            return;
        }

        if (kafkaConsumer != null) {
            kafkaConsumer.unsubscribe();
        }
        if (kafkaConsumer2 != null) {
            kafkaConsumer2.unsubscribe();
        }
    }

    /**
     * 获取数据，自动进行确认
     *
     * @return
     */
    public List<Message> get() {
        return get(100L, TimeUnit.MILLISECONDS);
    }

    public List<Message> get(Long timeout, TimeUnit unit) {
        waitClientRunning();
        if (!running) {
            return null;
        }

        List<Message> messages = getWithoutAck(timeout, unit);
        this.ack();
        return messages;
    }

    public List<Message> getWithoutAck() {
        return getWithoutAck(100L, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取数据，不进行确认，等待处理完成手工确认
     *
     * @return
     */
    public List<Message> getWithoutAck(Long timeout, TimeUnit unit) {
        waitClientRunning();
        if (!running) {
            return null;
        }

        ConsumerRecords<String, Message> records = kafkaConsumer.poll(unit.toMillis(timeout)); // 基于配置，最多只能poll到一条数据

        if (!records.isEmpty()) {
            // return records.iterator().next().value();
            List<Message> messages = new ArrayList<>();
            for (ConsumerRecord<String, Message> record : records) {
                messages.add(record.value());
            }
            return messages;
        }
        return null;
    }

    public List<FlatMessage> getFlatMessageWithoutAck(Long timeout, TimeUnit unit) {
        waitClientRunning();
        if (!running) {
            return null;
        }

        ConsumerRecords<String, String> records = kafkaConsumer2.poll(unit.toMillis(timeout));

        if (!records.isEmpty()) {
            List<FlatMessage> flatMessages = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                String flatMessageJson = record.value();
                FlatMessage flatMessage = JSON.parseObject(flatMessageJson, FlatMessage.class);
                flatMessages.add(flatMessage);
            }

            return flatMessages;
        }
        return null;
    }

    /**
     * 提交offset，如果超过 session.timeout.ms 设置的时间没有ack则会抛出异常，ack失败
     */
    public void ack() {
        waitClientRunning();
        if (!running) {
            return;
        }

        if (kafkaConsumer != null) {
            kafkaConsumer.commitSync();
        }
        if (kafkaConsumer2 != null) {
            kafkaConsumer2.commitSync();
        }
    }

    public void stopRunning() {
        if (running) {
            running = false; // 设置为非running状态
            // if (!mutex.state()) {
            // mutex.set(true); // 中断阻塞
            // }
        }
    }
}

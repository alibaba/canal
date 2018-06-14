package com.alibaba.otter.canal.kafka.client;

import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * canal kafka 数据操作客户端
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class KafkaCanalConnector {

    private KafkaConsumer<String, Message> kafkaConsumer;

    private String topic;

    private Integer partition;


    private Properties properties;

    public KafkaCanalConnector(String servers, String topic, Integer partition, String groupId) {
        this.topic = topic;
        this.partition = partition;

        properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", false);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest"); //earliest //如果没有offset则从最后的offset开始读
        properties.put("request.timeout.ms", "40000"); //必须大于session.timeout.ms的设置
        properties.put("session.timeout.ms", "30000"); //默认为30秒
        properties.put("max.poll.records", "1"); //所以一次只取一条数据
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", MessageDeserializer.class.getName());
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
     * 关闭链接
     */
    public void close() {
        kafkaConsumer.close();
    }

    /**
     * 订阅topic
     */
    public void subscribe() {
        if (kafkaConsumer == null) {
            kafkaConsumer = new KafkaConsumer<String, Message>(properties);
        }
        if (partition == null) {
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        } else {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            kafkaConsumer.assign(Collections.singletonList(topicPartition));
        }
    }

    /**
     * 取消订阅
     */
    public void unsubscribe() {
        kafkaConsumer.unsubscribe();
    }

    /**
     * 获取数据，自动进行确认
     *
     * @return
     */
    public Message get() {
        return get(100L, TimeUnit.MILLISECONDS);
    }

    public Message get(Long timeout, TimeUnit unit) {
        Message message = getWithoutAck(timeout, unit);
        this.ack();
        return message;
    }

    public Message getWithoutAck() {
        return getWithoutAck(100L, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取数据，不进行确认，等待处理完成手工确认
     *
     * @return
     */
    public Message getWithoutAck(Long timeout, TimeUnit unit) {
        ConsumerRecords<String, Message> records =
                kafkaConsumer.poll(unit.toMillis(timeout)); //基于配置，最多只能poll到一条数据

        if (!records.isEmpty()) {
            return records.iterator().next().value();
        }
        return null;
    }

    /**
     * 提交offset，如果超过 session.timeout.ms 设置的时间没有ack则会抛出异常，ack失败
     */
    public void ack() {
        kafkaConsumer.commitSync();
    }
}

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
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Lists;

/**
 * canal kafka 数据操作客户端
 * 
 * <pre>
 * 注意点：
 * 1. 相比于canal {@linkplain SimpleCanalConnector}, 这里get和ack操作不能有并发, 必须是一个线程执行get后，内存里执行完毕ack后再取下一个get
 * </pre>
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.1.1
 */
public class KafkaCanalConnector implements CanalMQConnector {

    private KafkaConsumer<String, Message> kafkaConsumer;
    private KafkaConsumer<String, String>  kafkaConsumer2;   // 用于扁平message的数据消费
    private String                         topic;
    private Integer                        partition;
    private Properties                     properties;
    private volatile boolean               connected = false;
    private volatile boolean               running   = false;
    private boolean                        flatMessage;

    public KafkaCanalConnector(String servers, String topic, Integer partition, String groupId, Integer batchSize,
                               boolean flatMessage){
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
        if (batchSize == null) {
            batchSize = 100;
        }
        properties.put("max.poll.records", batchSize.toString());
        properties.put("key.deserializer", StringDeserializer.class.getName());
        if (!flatMessage) {
            properties.put("value.deserializer", MessageDeserializer.class.getName());
        } else {
            properties.put("value.deserializer", StringDeserializer.class.getName());
        }
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
            kafkaConsumer = null;
        }
        if (kafkaConsumer2 != null) {
            kafkaConsumer2.close();
            kafkaConsumer2 = null;
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

    @Override
    public List<Message> getList(Long timeout, TimeUnit unit) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return Lists.newArrayList();
        }

        List<Message> messages = getListWithoutAck(timeout, unit);
        if (messages != null && !messages.isEmpty()) {
            this.ack();
        }
        return messages;
    }

    @Override
    public List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return Lists.newArrayList();
        }

        ConsumerRecords<String, Message> records = kafkaConsumer.poll(unit.toMillis(timeout)); // 基于配置，最多只能poll到一条数据

        if (!records.isEmpty()) {
            List<Message> messages = new ArrayList<>();
            for (ConsumerRecord<String, Message> record : records) {
                messages.add(record.value());
            }
            return messages;
        }
        return Lists.newArrayList();
    }

    @Override
    public List<FlatMessage> getFlatList(Long timeout, TimeUnit unit) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return Lists.newArrayList();
        }

        List<FlatMessage> messages = getFlatListWithoutAck(timeout, unit);
        if (messages != null && !messages.isEmpty()) {
            this.ack();
        }
        return messages;
    }

    @Override
    public List<FlatMessage> getFlatListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return Lists.newArrayList();
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
        return Lists.newArrayList();
    }

    @Override
    public void rollback() throws CanalClientException {
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

    @Override
    public void subscribe(String filter) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message get(int batchSize) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message getWithoutAck(int batchSize) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void ack(long batchId) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
    }

    @Override
    public void rollback(long batchId) throws CanalClientException {
        throw new CanalClientException("mq not support this method");
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

}

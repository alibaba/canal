package com.alibaba.otter.canal.kafka.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * canal kafka 数据操作客户端
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class KafkaCanalConnector implements CanalConnector {

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
        properties.put("auto.commit.interval.ms", 1000);
        properties.put("auto.offset.reset", "latest"); //earliest //如果没有offset则从最后的offset开始读
        properties.put("request.timeout.ms", 600000);
        properties.put("offsets.commit.timeout.ms", 300000);
        properties.put("session.timeout.ms", 30000);
        properties.put("max.poll.records", 1); //一次只取一条message
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", MessageDeserializer.class.getName());
    }

    @Override
    public void connect() throws CanalClientException {
        kafkaConsumer = new KafkaConsumer<String, Message>(properties);
    }

    @Override
    public void disconnect() throws CanalClientException {
        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.close();
            } catch (ConcurrentModificationException e) {
                kafkaConsumer.wakeup(); //通过wakeup异常间接关闭consumer
            }
        }
    }

    @Override
    public boolean checkValid() throws CanalClientException {
        return true;
    }

    @Override
    public void subscribe(String filter) throws CanalClientException {
        try {
            if (kafkaConsumer == null) {
                throw new CanalClientException("connect the kafka first before subscribe");
            }
            if (partition == null) {
                kafkaConsumer.subscribe(Collections.singletonList(topic));
            } else {
                kafkaConsumer.subscribe(Collections.singletonList(topic));
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                kafkaConsumer.assign(Collections.singletonList(topicPartition));
            }
        } catch (WakeupException e) {
            closeByWakeupException(e);
        } catch (Exception e) {
            throw new CanalClientException(e);
        }
    }

    @Override
    public void subscribe() throws CanalClientException {
        subscribe(null);
    }

    @Override
    public void unsubscribe() throws CanalClientException {
        try {
            kafkaConsumer.unsubscribe();
        } catch (WakeupException e) {
            closeByWakeupException(e);
        } catch (Exception e) {
            throw new CanalClientException(e);
        }
    }

    @Override
    public Message get(int batchSize) throws CanalClientException {
        return get(batchSize, 100L, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        Message message = getWithoutAck(batchSize, timeout, unit);
        this.ack(1);
        return message;
    }

    @Override
    public Message getWithoutAck(int batchSize) throws CanalClientException {
        return getWithoutAck(batchSize, 100L, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        try {
            ConsumerRecords<String, Message> records = kafkaConsumer.poll(unit.toMillis(timeout)); //基于配置，一次最多只能poll到一条Msg

            if (!records.isEmpty()) {
                return records.iterator().next().value();
            }
        } catch (WakeupException e) {
            closeByWakeupException(e);
        } catch (Exception e) {
            throw new CanalClientException(e);
        }
        return null;
    }

    @Override
    public void ack(long batchId) throws CanalClientException {
        try {
            kafkaConsumer.commitSync();
        } catch (WakeupException e) {
            closeByWakeupException(e);
        } catch (Exception e) {
            throw new CanalClientException(e);
        }
    }

    @Override
    public void rollback(long batchId) throws CanalClientException {

    }

    @Override
    public void rollback() throws CanalClientException {

    }

    @Override
    public void stopRunning() throws CanalClientException {

    }

    private void closeByWakeupException(WakeupException e) {
        kafkaConsumer.close();
        throw e;
    }
}

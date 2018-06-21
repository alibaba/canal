package com.alibaba.otter.canal.kafka.client;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.kafka.client.running.ClientRunningData;
import com.alibaba.otter.canal.kafka.client.running.ClientRunningListener;
import com.alibaba.otter.canal.kafka.client.running.ClientRunningMonitor;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
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
    private ClientRunningMonitor runningMonitor;  // 运行控制
    private ZkClientx zkClientx;
    private BooleanMutex mutex = new BooleanMutex(false);
    private volatile boolean connected = false;
    private volatile boolean running = false;

    public KafkaCanalConnector(String zkServers, String servers, String topic, Integer partition, String groupId) {
        this.topic = topic;
        this.partition = partition;

        properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", false);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest"); //如果没有offset则从最后的offset开始读
        properties.put("request.timeout.ms", "40000"); // 必须大于session.timeout.ms的设置
        properties.put("session.timeout.ms", "30000"); // 默认为30秒
        properties.put("max.poll.records", "1"); // 所以一次只取一条数据
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", MessageDeserializer.class.getName());

        if (zkServers != null) {
            zkClientx = new ZkClientx(zkServers);

            ClientRunningData clientData = new ClientRunningData();
            clientData.setGroupId(groupId);
            clientData.setAddress(AddressUtils.getHostIp());

            runningMonitor = new ClientRunningMonitor();
            runningMonitor.setTopic(topic);
            runningMonitor.setZkClient(zkClientx);
            runningMonitor.setClientData(clientData);
            runningMonitor.setListener(new ClientRunningListener() {
                public void processActiveEnter() {
                    mutex.set(true);
                }

                public void processActiveExit() {
                    mutex.set(false);
                }
            });
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

        if (runningMonitor != null) {
            if (!runningMonitor.isStart()) {
                runningMonitor.start();
            }
        }

        connected = true;

        if (kafkaConsumer == null) {
            kafkaConsumer = new KafkaConsumer<String, Message>(properties);
        }
    }

    /**
     * 关闭链接
     */
    public void disconnnect() {
        kafkaConsumer.close();

        connected = false;
        if (runningMonitor.isStart()) {
            runningMonitor.stop();
        }
    }

    private void waitClientRunning() {
        try {
            if (zkClientx != null) {
                if (!connected) {// 未调用connect
                    throw new CanalClientException("should connect first");
                }

                running = true;
                mutex.get();// 阻塞等待
            } else {
                // 单机模式直接设置为running
                running = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CanalClientException(e);
        }
    }

    public boolean checkValid() {
        if (zkClientx != null) {
            return mutex.state();
        } else {
            return true;// 默认都放过
        }
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
        waitClientRunning();
        if (!running) {
            return;
        }

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
        waitClientRunning();
        if (!running) {
            return null;
        }

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
        waitClientRunning();
        if (!running) {
            return null;
        }

        ConsumerRecords<String, Message> records = kafkaConsumer.poll(unit.toMillis(timeout)); // 基于配置，最多只能poll到一条数据

        if (!records.isEmpty()) {
            return records.iterator().next().value();
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

        kafkaConsumer.commitSync();
    }

    public void stopRunning() {
        if (running) {
            running = false; // 设置为非running状态
            if (!mutex.state()) {
                mutex.set(true); // 中断阻塞
            }
        }
    }
}

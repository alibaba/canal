package com.alibaba.otter.canal.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.alibaba.otter.canal.common.CanalMessageSerializer;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Kafka Message类的序列化
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class MessageSerializer implements Serializer<Message> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Message data) {
        return CanalMessageSerializer.serializer(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}

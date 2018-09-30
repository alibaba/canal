package com.alibaba.otter.canal.client.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Kafka Message类的反序列化
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class MessageDeserializer implements Deserializer<Message> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic1, byte[] data) {
        return CanalMessageDeserializer.deserializer(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}

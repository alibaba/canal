package com.alibaba.otter.canal.connector.kafka.consumer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.alibaba.otter.canal.connector.core.util.CanalMessageSerializerUtil;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Kafka Message类的反序列化
 *
 * @author rewerma @ 2018-6-12
 * @version 1.0.0
 */
public class KafkaMessageDeserializer implements Deserializer<Message> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic1, byte[] data) {
        return CanalMessageSerializerUtil.deserializer(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}

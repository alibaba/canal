package com.alibaba.otter.canal.client.kafka;

import com.alibaba.otter.canal.client.DeserializerUtil;
import com.alibaba.otter.canal.protocol.Message;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Message类的反序列化
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public class MessageDeserializer implements Deserializer<Message> {

    private static Logger logger = LoggerFactory.getLogger(MessageDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic1, byte[] data) {
        return DeserializerUtil.deserializer(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}

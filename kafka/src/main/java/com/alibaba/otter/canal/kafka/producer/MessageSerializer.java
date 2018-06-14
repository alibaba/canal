package com.alibaba.otter.canal.kafka.producer;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
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
        try {
            if (data == null) return null;
            else {
                CanalPacket.Messages.Builder messageBuilder = CanalPacket.Messages.newBuilder();
                if (data.getId() != -1 && !CollectionUtils.isEmpty(data.getEntries())) {
                    for (CanalEntry.Entry entry : data.getEntries()) {
                        messageBuilder.addMessages(entry.toByteString());
                    }
                }
                CanalPacket.Packet.Builder packetBuilder = CanalPacket.Packet.newBuilder();
                packetBuilder.setType(CanalPacket.PacketType.MESSAGES);
                packetBuilder.setBody(messageBuilder.build().toByteString());
                return packetBuilder.build().toByteArray();
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing message to byte[] ");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}

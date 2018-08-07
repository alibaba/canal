package com.alibaba.otter.canal.kafka.client;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;

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
    public Message deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            else {
                CanalPacket.Packet p = CanalPacket.Packet.parseFrom(data);
                switch (p.getType()) {
                    case MESSAGES: {
                        if (!p.getCompression().equals(CanalPacket.Compression.NONE)) {
                            throw new CanalClientException("compression is not supported in this connector");
                        }

                        CanalPacket.Messages messages = CanalPacket.Messages.parseFrom(p.getBody());
                        Message result = new Message(messages.getBatchId());
                        for (ByteString byteString : messages.getMessagesList()) {
                            result.addEntry(CanalEntry.Entry.parseFrom(byteString));
                        }
                        return result;
                    }
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("Error when deserializing byte[] to message ");
        }
        return null;
    }

    @Override
    public void close() {
        // nothing to do
    }
}

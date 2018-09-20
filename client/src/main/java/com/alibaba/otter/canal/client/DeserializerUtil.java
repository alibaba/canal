package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeserializerUtil {
    private static Logger logger = LoggerFactory.getLogger(DeserializerUtil.class);

    public static Message deserializer(byte[] data) {
        try {
            if (data == null) {
                return null;
            } else {
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
            logger.error("Error when deserializing byte[] to message ", e);
        }
        return null;
    }
}

package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.CanalPacket.Ack;
import com.alibaba.otter.canal.protocol.CanalPacket.Compression;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;

public class CanalMessageDeserializer {

    public static Message deserializer(byte[] data) {
        return deserializer(data, false);
    }

    public static Message deserializer(byte[] data, boolean lazyParseEntry) {
        try {
            if (data == null) {
                return null;
            } else {
                CanalPacket.Packet p = CanalPacket.Packet.parseFrom(data);
                switch (p.getType()) {
                    case MESSAGES: {
                        if (!p.getCompression().equals(Compression.NONE)
                            && !p.getCompression().equals(Compression.COMPRESSIONCOMPATIBLEPROTO2)) {
                            throw new CanalClientException("compression is not supported in this connector");
                        }

                        CanalPacket.Messages messages = CanalPacket.Messages.parseFrom(p.getBody());
                        Message result = new Message(messages.getBatchId());
                        if (lazyParseEntry) {
                            // byteString
                            result.setRawEntries(messages.getMessagesList());
                            result.setRaw(true);
                        } else {
                            for (ByteString byteString : messages.getMessagesList()) {
                                result.addEntry(CanalEntry.Entry.parseFrom(byteString));
                            }
                            result.setRaw(false);
                        }
                        return result;
                    }
                    case ACK: {
                        Ack ack = Ack.parseFrom(p.getBody());
                        throw new CanalClientException("something goes wrong with reason: " + ack.getErrorMessage());
                    }
                    default: {
                        throw new CanalClientException("unexpected packet type: " + p.getType());
                    }
                }
            }
        } catch (Exception e) {
            throw new CanalClientException("deserializer failed", e);
        }
    }
}

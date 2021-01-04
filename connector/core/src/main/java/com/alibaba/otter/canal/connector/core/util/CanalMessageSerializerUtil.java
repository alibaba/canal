package com.alibaba.otter.canal.connector.core.util;

import java.util.List;

import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.CanalPacket.PacketType;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

/**
 * Canal message 序列化工具类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class CanalMessageSerializerUtil {

    @SuppressWarnings("deprecation")
    public static byte[] serializer(Message data, boolean filterTransactionEntry) {
        try {
            if (data != null) {
                if (data.getId() != -1) {
                    if (data.isRaw() && !CollectionUtils.isEmpty(data.getRawEntries())) {
                        // for performance
                        List<ByteString> rowEntries = data.getRawEntries();
                        // message size
                        int messageSize = 0;
                        messageSize += CodedOutputStream.computeInt64Size(1, data.getId());

                        int dataSize = 0;
                        for (ByteString rowEntry : rowEntries) {
                            dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);
                        }
                        messageSize += dataSize;
                        messageSize += 1 * rowEntries.size();
                        // packet size
                        int size = 0;
                        size += CodedOutputStream.computeEnumSize(3, PacketType.MESSAGES.getNumber());
                        size += CodedOutputStream.computeTagSize(5)
                                + CodedOutputStream.computeRawVarint32Size(messageSize) + messageSize;
                        // build data
                        byte[] body = new byte[size];
                        CodedOutputStream output = CodedOutputStream.newInstance(body);
                        output.writeEnum(3, PacketType.MESSAGES.getNumber());

                        output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                        output.writeRawVarint32(messageSize);
                        // message
                        output.writeInt64(1, data.getId());
                        for (ByteString rowEntry : rowEntries) {
                            output.writeBytes(2, rowEntry);
                        }
                        output.checkNoSpaceLeft();
                        return body;
                    } else if (!CollectionUtils.isEmpty(data.getEntries())) {
                        // mq模式只会走到非rawEntry模式
                        CanalPacket.Messages.Builder messageBuilder = CanalPacket.Messages.newBuilder();
                        for (CanalEntry.Entry entry : data.getEntries()) {
                            if (filterTransactionEntry
                                && (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND)) {
                                continue;
                            }

                            messageBuilder.addMessages(entry.toByteString());
                        }

                        CanalPacket.Packet.Builder packetBuilder = CanalPacket.Packet.newBuilder();
                        packetBuilder.setType(PacketType.MESSAGES);
                        packetBuilder.setVersion(1);
                        packetBuilder.setBody(messageBuilder.build().toByteString());
                        return packetBuilder.build().toByteArray();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error when serializing message to byte[] ");
        }
        return null;
    }

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
                        if (!p.getCompression().equals(CanalPacket.Compression.NONE)
                            && !p.getCompression().equals(CanalPacket.Compression.COMPRESSIONCOMPATIBLEPROTO2)) {
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
                        CanalPacket.Ack ack = CanalPacket.Ack.parseFrom(p.getBody());
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

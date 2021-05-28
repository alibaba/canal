package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.CanalPacket.Compression;
import com.alibaba.otter.canal.protocol.CanalPacket.Messages;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.CanalPacket.PacketType;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ProtocolTest {

    @Test(expected = CanalClientException.class)
    public void testSimple() throws IOException {
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setLogfileName("mysql-bin.000001");
        headerBuilder.setLogfileOffset(1024);
        headerBuilder.setExecuteTime(1024);
        Entry.Builder entryBuilder = Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        entryBuilder.setEntryType(EntryType.ROWDATA);
        Entry entry = entryBuilder.build();
        Message message = new Message(3, true, Arrays.asList(entry.toByteString()));

        byte[] body = buildData(message);
        Packet packet = Packet.parseFrom(body);
        switch (packet.getType()) {
            case MESSAGES: {
                if (!packet.getCompression().equals(Compression.NONE)) {
                    throw new CanalClientException("compression is not supported in this connector");
                }

                Messages messages = Messages.parseFrom(packet.getBody());
                Message result = new Message(messages.getBatchId());
                for (ByteString byteString : messages.getMessagesList()) {
                    result.addEntry(Entry.parseFrom(byteString));
                }

                System.out.println(result);
                break;
            }
            default: {
                throw new CanalClientException("unexpected packet type: " + packet.getType());
            }
        }
    }

    @SuppressWarnings("deprecation")
    private byte[] buildData(Message message) throws IOException {
        List<ByteString> rowEntries = message.getRawEntries();
        // message size
        int messageSize = 0;
        messageSize += com.google.protobuf.CodedOutputStream.computeInt64Size(1, message.getId());

        int dataSize = 0;
        for (ByteString rowEntry : rowEntries) {
            dataSize += CodedOutputStream.computeBytesSizeNoTag(rowEntry);
        }
        messageSize += dataSize;
        messageSize += 1 * rowEntries.size();
        // packet size
        int size = 0;
        size += com.google.protobuf.CodedOutputStream.computeEnumSize(3, PacketType.MESSAGES.getNumber());
        size += com.google.protobuf.CodedOutputStream.computeTagSize(5)
                + com.google.protobuf.CodedOutputStream.computeRawVarint32Size(messageSize) + messageSize;
        // TODO recyle bytes[]
        byte[] body = new byte[size];
        CodedOutputStream output = CodedOutputStream.newInstance(body);
        output.writeEnum(3, PacketType.MESSAGES.getNumber());

        output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        output.writeRawVarint32(messageSize);
        // message
        output.writeInt64(1, message.getId());
        for (ByteString rowEntry : rowEntries) {
            output.writeBytes(2, rowEntry);
        }
        output.checkNoSpaceLeft();

        return body;
    }
}

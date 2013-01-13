package com.alibaba.otter.canal.parse.inbound.mysql.networking.utils;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.protocol.position.LogPosition;

public class BinlogDumpCommandBuilder {

    public BinlogDumpCommandPacket build(LogPosition logPosition) {
        BinlogDumpCommandPacket command = new BinlogDumpCommandPacket();
        command.binlogPosition = logPosition.getPostion().getPosition();
        if (!StringUtils.isEmpty(logPosition.getPostion().getJournalName())) {
            command.binlogFileName = logPosition.getPostion().getJournalName();
        }
        command.slaveServerId = logPosition.getIdentity().getSlaveId();
        // end settings.
        return command;
    }

    public ChannelBuffer toChannelBuffer(BinlogDumpCommandPacket command) throws IOException {
        byte[] commandBytes = command.toBytes();
        byte[] headerBytes = assembleHeaderBytes(commandBytes.length);
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(headerBytes, commandBytes);
        return buffer;
    }

    private byte[] assembleHeaderBytes(int length) {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(length);
        header.setPacketSequenceNumber((byte) 0x00);
        return header.toBytes();
    }
}

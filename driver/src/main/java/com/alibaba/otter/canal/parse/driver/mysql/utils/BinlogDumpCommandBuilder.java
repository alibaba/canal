package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;

public class BinlogDumpCommandBuilder {

    public BinlogDumpCommandPacket build(String binglogFile, long position, long slaveId) {
        BinlogDumpCommandPacket command = new BinlogDumpCommandPacket();
        command.binlogPosition = position;
        if (!StringUtils.isEmpty(binglogFile)) {
            command.binlogFileName = binglogFile;
        }
        command.slaveServerId = slaveId;
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

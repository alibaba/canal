package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.IPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;

public class ChannelBufferHelper {

    protected transient final Logger logger = LoggerFactory.getLogger(ChannelBufferHelper.class);

    public final HeaderPacket assembleHeaderPacket(ChannelBuffer buffer) {
        HeaderPacket header = new HeaderPacket();
        byte[] headerBytes = new byte[MSC.HEADER_PACKET_LENGTH];
        buffer.readBytes(headerBytes);
        header.fromBytes(headerBytes);
        return header;
    }

    public final PacketWithHeaderPacket assembleBodyPacketWithHeader(ChannelBuffer buffer, HeaderPacket header,
                                                                     PacketWithHeaderPacket body) throws IOException {
        if (body.getHeader() == null) {
            body.setHeader(header);
        }
        logger.debug("body packet type:{}", body.getClass());
        logger.debug("read body packet with packet length: {} ", header.getPacketBodyLength());
        byte[] packetBytes = new byte[header.getPacketBodyLength()];

        logger.debug("readable bytes before reading body:{}", buffer.readableBytes());
        buffer.readBytes(packetBytes);
        body.fromBytes(packetBytes);

        logger.debug("body packet: {}", body);
        return body;
    }

    public final ChannelBuffer createHeaderWithPacketNumberPlusOne(int bodyLength, byte packetNumber) {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(bodyLength);
        header.setPacketSequenceNumber((byte) (packetNumber + 1));
        return ChannelBuffers.wrappedBuffer(header.toBytes());
    }

    public final ChannelBuffer createHeader(int bodyLength, byte packetNumber) {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(bodyLength);
        header.setPacketSequenceNumber(packetNumber);
        return ChannelBuffers.wrappedBuffer(header.toBytes());
    }

    public final ChannelBuffer buildChannelBufferFromCommandPacket(IPacket packet) throws IOException {
        byte[] bodyBytes = packet.toBytes();
        ChannelBuffer header = createHeader(bodyBytes.length, (byte) 0);
        return ChannelBuffers.wrappedBuffer(header, ChannelBuffers.wrappedBuffer(bodyBytes));
    }
}

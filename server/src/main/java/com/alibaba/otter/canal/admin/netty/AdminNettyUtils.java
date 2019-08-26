package com.alibaba.otter.canal.admin.netty;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.CompositeChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;

import com.alibaba.otter.canal.protocol.AdminPacket;
import com.alibaba.otter.canal.protocol.AdminPacket.Ack;
import com.alibaba.otter.canal.protocol.AdminPacket.Packet;

public class AdminNettyUtils {

    public static int HEADER_LENGTH = 4;
    public static int VERSION       = 1;

    public static void write(Channel channel, ByteBuffer body) {
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN).putInt(body.limit()).array();
        List<ChannelBuffer> components = new ArrayList<ChannelBuffer>(2);
        components.add(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, header));
        components.add(ChannelBuffers.wrappedBuffer(body));
        Channels.write(channel, new CompositeChannelBuffer(ByteOrder.BIG_ENDIAN, components));
    }

    public static void write(Channel channel, byte[] body) {
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN).putInt(body.length).array();
        Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body));
    }

    public static void write(Channel channel, byte[] body, ChannelFutureListener channelFutureListner) {
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN).putInt(body.length).array();
        Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body)).addListener(channelFutureListner);
    }

    public static byte[] ackPacket() {
        return ackPacket(null);
    }

    public static byte[] ackPacket(String message) {
        return Packet.newBuilder()
            .setType(AdminPacket.PacketType.ACK)
            .setVersion(VERSION)
            .setBody(Ack.newBuilder().setCode(0).setMessage(message == null ? "" : message).build().toByteString())
            .build()
            .toByteArray();
    }

    public static byte[] errorPacket(int errorCode, String errorMessage) {
        return Packet.newBuilder()
            .setType(AdminPacket.PacketType.ACK)
            .setVersion(VERSION)
            .setBody(Ack.newBuilder().setCode(errorCode).setMessage(errorMessage).build().toByteString())
            .build()
            .toByteArray();
    }
}

package com.alibaba.otter.canal.server.netty;

import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.alibaba.otter.canal.protocol.E3;
import com.alibaba.otter.canal.protocol.E3.Ack;
import com.alibaba.otter.canal.protocol.E3.E3Packet;

public class NettyUtils {

    private static int  HEADER_LENGTH    = 4;
    public static Timer hashedWheelTimer = new HashedWheelTimer();

    public static void write(Channel channel, byte[] body, ChannelFutureListener channelFutureListner) {
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).putInt(body.length).array();
        if (channelFutureListner == null) {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body));
        } else {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body)).addListener(channelFutureListner);
        }
    }

    public static void ack(Channel channel, ChannelFutureListener channelFutureListner) {
        write(
              channel,
              E3Packet.newBuilder().setType(E3.PacketType.ACK).setBody(Ack.newBuilder().build().toByteString()).build().toByteArray(),
              channelFutureListner);
    }

    public static void error(int errorCode, String errorMessage, Channel channel,
                             ChannelFutureListener channelFutureListener) {
        if (channelFutureListener == null) {
            channelFutureListener = ChannelFutureListener.CLOSE;
        }

        write(
              channel,
              E3Packet.newBuilder().setType(E3.PacketType.ACK).setBody(
                                                                       Ack.newBuilder().setErrorCode(errorCode).setErrorMessage(
                                                                                                                                errorMessage).build().toByteString()).build().toByteArray(),
              channelFutureListener);
    }
}

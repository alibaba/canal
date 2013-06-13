package com.alibaba.otter.canal.server.netty;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.CanalPacket.Ack;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;

public class NettyUtils {

    private static final Logger logger           = LoggerFactory.getLogger(NettyUtils.class);
    private static int          HEADER_LENGTH    = 4;
    public static Timer         hashedWheelTimer = new HashedWheelTimer();

    public static void write(Channel channel, byte[] body, ChannelFutureListener channelFutureListner) {
        byte[] header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN).putInt(body.length).array();
        if (channelFutureListner == null) {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body));
        } else {
            Channels.write(channel, ChannelBuffers.wrappedBuffer(header, body)).addListener(channelFutureListner);
        }
    }

    public static void ack(Channel channel, ChannelFutureListener channelFutureListner) {
        write(
              channel,
              Packet.newBuilder().setType(CanalPacket.PacketType.ACK).setBody(Ack.newBuilder().build().toByteString()).build().toByteArray(),
              channelFutureListner);
    }

    public static void error(int errorCode, String errorMessage, Channel channel,
                             ChannelFutureListener channelFutureListener) {
        if (channelFutureListener == null) {
            channelFutureListener = ChannelFutureListener.CLOSE;
        }

        logger.error("ErrotCode:{} , Caused by : \n{}", errorCode, errorMessage);
        write(
              channel,
              Packet.newBuilder().setType(CanalPacket.PacketType.ACK).setBody(
                                                                              Ack.newBuilder().setErrorCode(errorCode).setErrorMessage(
                                                                                                                                       errorMessage).build().toByteString()).build().toByteArray(),
              channelFutureListener);
    }
}

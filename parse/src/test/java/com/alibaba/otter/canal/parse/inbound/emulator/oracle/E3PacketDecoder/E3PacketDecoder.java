package com.alibaba.otter.canal.parse.inbound.emulator.oracle.E3PacketDecoder;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.alibaba.otter.canal.protocol.E3;

/**
 * decode e3 packets
 * 
 * @author: yuanzu Date: 12-9-24 Time: 下午5:35
 */
public class E3PacketDecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < 4) {
            return null;
        }

        buffer.markReaderIndex();
        int dataLength = buffer.readInt();

        if (buffer.readableBytes() < dataLength) {
            buffer.resetReaderIndex();
            return null;
        }

        byte[] bytes = new byte[dataLength];
        buffer.readBytes(bytes);

        return E3.E3Packet.parseFrom(bytes);
    }
}

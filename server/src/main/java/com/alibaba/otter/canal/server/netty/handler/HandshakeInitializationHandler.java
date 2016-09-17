package com.alibaba.otter.canal.server.netty.handler;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.CanalPacket.Handshake;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.server.netty.NettyUtils;

/**
 * handshake交互
 * 
 * @author jianghang 2012-10-24 上午11:39:54
 * @version 1.0.0
 */
public class HandshakeInitializationHandler extends SimpleChannelHandler {

    private static final Logger logger = LoggerFactory.getLogger(HandshakeInitializationHandler.class);

    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        byte[] body = Packet.newBuilder()
            .setType(CanalPacket.PacketType.HANDSHAKE)
            .setBody(Handshake.newBuilder().build().toByteString())
            .build()
            .toByteArray();
        NettyUtils.write(ctx.getChannel(), body, null);
        logger.info("send handshake initialization packet to : {}", ctx.getChannel());
    }
}

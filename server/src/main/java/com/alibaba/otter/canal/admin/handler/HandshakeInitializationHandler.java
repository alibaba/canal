package com.alibaba.otter.canal.admin.handler;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.netty.AdminNettyUtils;
import com.alibaba.otter.canal.protocol.AdminPacket;
import com.alibaba.otter.canal.protocol.AdminPacket.Handshake;
import com.alibaba.otter.canal.protocol.AdminPacket.Packet;
import com.google.protobuf.ByteString;

/**
 * handshake交互
 * 
 * @author agapple 2019年8月24日 下午10:58:34
 * @since 1.1.4
 */
public class HandshakeInitializationHandler extends SimpleChannelHandler {

    // support to maintain socket channel.
    private ChannelGroup childGroups;

    public HandshakeInitializationHandler(ChannelGroup childGroups){
        this.childGroups = childGroups;
    }

    private static final Logger logger = LoggerFactory.getLogger(HandshakeInitializationHandler.class);

    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // add new socket channel in channel container, used to manage sockets.
        if (childGroups != null) {
            childGroups.add(ctx.getChannel());
        }

        final byte[] seed = org.apache.commons.lang3.RandomUtils.nextBytes(8);
        byte[] body = Packet.newBuilder()
            .setType(AdminPacket.PacketType.HANDSHAKE)
            .setVersion(AdminNettyUtils.VERSION)
            .setBody(Handshake.newBuilder().setSeeds(ByteString.copyFrom(seed)).build().toByteString())
            .build()
            .toByteArray();

        AdminNettyUtils.write(ctx.getChannel(), body, future -> {
            logger.info("remove unused channel handlers after authentication is done successfully.");
            ctx.getPipeline().get(HandshakeInitializationHandler.class.getName());
            ClientAuthenticationHandler handler = (ClientAuthenticationHandler) ctx.getPipeline()
                .get(ClientAuthenticationHandler.class.getName());
            handler.setSeed(seed);
        });
        logger.info("send handshake initialization packet to : {}", ctx.getChannel());
    }
}

package com.alibaba.otter.canal.admin.handler;

import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.alibaba.otter.canal.admin.CanalAdmin;
import com.alibaba.otter.canal.admin.netty.AdminNettyUtils;
import com.alibaba.otter.canal.protocol.AdminPacket.ClientAuth;
import com.alibaba.otter.canal.protocol.AdminPacket.Packet;
import com.alibaba.otter.canal.server.netty.NettyUtils;

/**
 * 客户端身份认证处理
 * 
 * @author agapple 2019年8月24日 下午10:58:53
 * @since 1.1.4
 */
public class ClientAuthenticationHandler extends SimpleChannelHandler {

    private static final Logger logger                                  = LoggerFactory.getLogger(ClientAuthenticationHandler.class);
    private final int           SUPPORTED_VERSION                       = 3;
    private final int           defaultSubscriptorDisconnectIdleTimeout = 60 * 60 * 1000;
    private CanalAdmin          canalAdmin;
    private byte[]              seed;

    public ClientAuthenticationHandler(){

    }

    public ClientAuthenticationHandler(CanalAdmin canalAdmin){
        this.canalAdmin = canalAdmin;
    }

    public void messageReceived(final ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        final Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
        switch (packet.getVersion()) {
            case SUPPORTED_VERSION:
            default:
                final ClientAuth clientAuth = ClientAuth.parseFrom(packet.getBody());
                if (seed == null) {
                    byte[] errorBytes = AdminNettyUtils.errorPacket(300,
                        MessageFormatter.format("auth failed for seed is null", clientAuth.getUsername()).getMessage());
                    AdminNettyUtils.write(ctx.getChannel(), errorBytes);
                }

                if (!canalAdmin.auth(clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed)) {
                    byte[] errorBytes = AdminNettyUtils.errorPacket(300,
                        MessageFormatter.format("auth failed for user:{}", clientAuth.getUsername()).getMessage());
                    AdminNettyUtils.write(ctx.getChannel(), errorBytes);
                }

                byte[] ackBytes = AdminNettyUtils.ackPacket();
                AdminNettyUtils.write(ctx.getChannel(), ackBytes, future -> {
                    logger.info("remove unused channel handlers after authentication is done successfully.");
                    ctx.getPipeline().remove(HandshakeInitializationHandler.class.getName());
                    ctx.getPipeline().remove(ClientAuthenticationHandler.class.getName());

                    int readTimeout = defaultSubscriptorDisconnectIdleTimeout;
                    int writeTimeout = defaultSubscriptorDisconnectIdleTimeout;
                    if (clientAuth.getNetReadTimeout() > 0) {
                        readTimeout = clientAuth.getNetReadTimeout();
                    }
                    if (clientAuth.getNetWriteTimeout() > 0) {
                        writeTimeout = clientAuth.getNetWriteTimeout();
                    }
                    // fix bug: soTimeout parameter's unit from connector is
                    // millseconds.
                    IdleStateHandler idleStateHandler = new IdleStateHandler(NettyUtils.hashedWheelTimer,
                        readTimeout,
                        writeTimeout,
                        0,
                        TimeUnit.MILLISECONDS);
                    ctx.getPipeline().addBefore(SessionHandler.class.getName(),
                        IdleStateHandler.class.getName(),
                        idleStateHandler);

                    IdleStateAwareChannelHandler idleStateAwareChannelHandler = new IdleStateAwareChannelHandler() {

                        public void channelIdle(ChannelHandlerContext ctx1, IdleStateEvent e1) throws Exception {
                            logger.warn("channel:{} idle timeout exceeds, close channel to save server resources...",
                                ctx1.getChannel());
                            ctx1.getChannel().close();
                        }

                    };
                    ctx.getPipeline().addBefore(SessionHandler.class.getName(),
                        IdleStateAwareChannelHandler.class.getName(),
                        idleStateAwareChannelHandler);
                });
                break;
        }
    }

    public void setCanalAdmin(CanalAdmin canalAdmin) {
        this.canalAdmin = canalAdmin;
    }

    public void setSeed(byte[] seed) {
        this.seed = seed;
    }

}

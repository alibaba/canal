package com.alibaba.otter.canal.server.netty.handler;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.helpers.MessageFormatter;

import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAuth;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.NettyUtils;

/**
 * 客户端身份认证处理
 * 
 * @author jianghang 2012-10-24 上午11:12:45
 * @version 1.0.0
 */
public class ClientAuthenticationHandler extends SimpleChannelHandler {

    private static final Logger     logger                                  = LoggerFactory.getLogger(ClientAuthenticationHandler.class);
    private final int               SUPPORTED_VERSION                       = 3;
    private final int               defaultSubscriptorDisconnectIdleTimeout = 60 * 60 * 1000;
    private CanalServerWithEmbedded embeddedServer;
    private byte[]                  seed;

    public ClientAuthenticationHandler(){

    }

    public ClientAuthenticationHandler(CanalServerWithEmbedded embeddedServer){
        this.embeddedServer = embeddedServer;
    }

    public void messageReceived(final ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        final Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
        switch (packet.getVersion()) {
            case SUPPORTED_VERSION:
            default:
                final ClientAuth clientAuth = ClientAuth.parseFrom(packet.getBody());
                if (seed == null) {
                    byte[] errorBytes = NettyUtils.errorPacket(400,
                        MessageFormatter.format("auth failed for seed is null", clientAuth.getUsername()).getMessage());
                    NettyUtils.write(ctx.getChannel(), errorBytes, null);
                }

                if (!embeddedServer.auth(clientAuth.getUsername(), clientAuth.getPassword().toStringUtf8(), seed)) {
                    byte[] errorBytes = NettyUtils.errorPacket(400,
                        MessageFormatter.format("auth failed for user:{}", clientAuth.getUsername()).getMessage());
                    NettyUtils.write(ctx.getChannel(), errorBytes, null);
                }

                // 如果存在订阅信息
                if (StringUtils.isNotEmpty(clientAuth.getDestination())
                    && StringUtils.isNotEmpty(clientAuth.getClientId())) {
                    ClientIdentity clientIdentity = new ClientIdentity(clientAuth.getDestination(),
                        Short.valueOf(clientAuth.getClientId()),
                        clientAuth.getFilter());
                    try {
                        MDC.put("destination", clientIdentity.getDestination());
                        embeddedServer.subscribe(clientIdentity);
                        // 尝试启动，如果已经启动，忽略
                        if (!embeddedServer.isStart(clientIdentity.getDestination())) {
                            ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(clientIdentity.getDestination());
                            if (!runningMonitor.isStart()) {
                                runningMonitor.start();
                            }
                        }
                    } finally {
                        MDC.remove("destination");
                    }
                }
                // 鉴权一次性，暂不统计
                NettyUtils.ack(ctx.getChannel(), future -> {
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

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

    public void setSeed(byte[] seed) {
        this.seed = seed;
    }

}

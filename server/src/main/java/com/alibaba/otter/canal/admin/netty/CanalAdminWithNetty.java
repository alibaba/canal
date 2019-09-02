package com.alibaba.otter.canal.admin.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.admin.CanalAdmin;
import com.alibaba.otter.canal.admin.handler.ClientAuthenticationHandler;
import com.alibaba.otter.canal.admin.handler.HandshakeInitializationHandler;
import com.alibaba.otter.canal.admin.handler.SessionHandler;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder;

/**
 * 基于netty网络服务的server实现
 * 
 * @author jianghang 2012-7-12 下午01:34:49
 * @version 1.0.0
 */
public class CanalAdminWithNetty extends AbstractCanalLifeCycle {

    private String          ip;
    private int             port;
    private Channel         serverChannel = null;
    private ServerBootstrap bootstrap     = null;
    private ChannelGroup    childGroups   = null; // socket channel
                                                  // container, used to
                                                  // close sockets
                                                  // explicitly.
    private CanalAdmin      canalAdmin;

    private static class SingletonHolder {

        private static final CanalAdminWithNetty CANAL_ADMIN_WITH_NETTY = new CanalAdminWithNetty();
    }

    private CanalAdminWithNetty(){
        this.childGroups = new DefaultChannelGroup();
    }

    public static CanalAdminWithNetty instance() {
        return SingletonHolder.CANAL_ADMIN_WITH_NETTY;
    }

    public void start() {
        super.start();

        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));
        /*
         * enable keep-alive mechanism, handle abnormal network connection
         * scenarios on OS level. the threshold parameters are depended on OS.
         * e.g. On Linux: net.ipv4.tcp_keepalive_time = 300
         * net.ipv4.tcp_keepalive_probes = 2 net.ipv4.tcp_keepalive_intvl = 30
         */
        bootstrap.setOption("child.keepAlive", true);
        /*
         * optional parameter.
         */
        bootstrap.setOption("child.tcpNoDelay", true);

        // 构造对应的pipeline
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipelines = Channels.pipeline();
                pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
                // support to maintain child socket channel.
                pipelines.addLast(HandshakeInitializationHandler.class.getName(),
                    new HandshakeInitializationHandler(childGroups));
                pipelines.addLast(ClientAuthenticationHandler.class.getName(),
                    new ClientAuthenticationHandler(canalAdmin));

                SessionHandler sessionHandler = new SessionHandler(canalAdmin);
                pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
                return pipelines;
            }
        });

        // 启动
        if (StringUtils.isNotEmpty(ip)) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
        }
    }

    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        // close sockets explicitly to reduce socket channel hung in complicated
        // network environment.
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(5000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setCanalAdmin(CanalAdmin canalAdmin) {
        this.canalAdmin = canalAdmin;
    }

}

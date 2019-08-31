package com.alibaba.otter.canal.admin.handler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.alibaba.otter.canal.admin.CanalAdmin;
import com.alibaba.otter.canal.admin.netty.AdminNettyUtils;
import com.alibaba.otter.canal.protocol.AdminPacket.InstanceAdmin;
import com.alibaba.otter.canal.protocol.AdminPacket.LogAdmin;
import com.alibaba.otter.canal.protocol.AdminPacket.Packet;
import com.alibaba.otter.canal.protocol.AdminPacket.ServerAdmin;

public class SessionHandler extends SimpleChannelHandler {

    private static final Logger logger = LoggerFactory.getLogger(SessionHandler.class);
    private CanalAdmin          canalAdmin;

    public SessionHandler(){
    }

    public SessionHandler(CanalAdmin canalAdmin){
        this.canalAdmin = canalAdmin;
    }

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        logger.info("message receives in session handler...");
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
        try {
            String action = null;
            String message = null;
            String destination = null;
            switch (packet.getType()) {
                case SERVER:
                    ServerAdmin serverAdmin = ServerAdmin.parseFrom(packet.getBody());
                    action = serverAdmin.getAction();
                    switch (action) {
                        case "check":
                            message = canalAdmin.check() ? "1" : "0";
                            break;
                        case "start":
                            message = canalAdmin.start() ? "1" : "0";
                            break;
                        case "stop":
                            message = canalAdmin.stop() ? "1" : "0";
                            break;
                        case "restart":
                            message = canalAdmin.restart() ? "1" : "0";
                            break;
                        case "list":
                            message = canalAdmin.getRunningInstances();
                            break;
                        default:
                            byte[] errorBytes = AdminNettyUtils.errorPacket(301,
                                MessageFormatter.format("ServerAdmin action={} is unknown", action).getMessage());
                            AdminNettyUtils.write(ctx.getChannel(), errorBytes);
                            break;
                    }
                    AdminNettyUtils.write(ctx.getChannel(), AdminNettyUtils.ackPacket(message));
                    break;
                case INSTANCE:
                    InstanceAdmin instanceAdmin = InstanceAdmin.parseFrom(packet.getBody());
                    destination = instanceAdmin.getDestination();
                    action = instanceAdmin.getAction();
                    switch (action) {
                        case "check":
                            message = canalAdmin.checkInstance(destination) ? "1" : "0";
                            break;
                        case "start":
                            message = canalAdmin.startInstance(destination) ? "1" : "0";
                            break;
                        case "stop":
                            message = canalAdmin.stopInstance(destination) ? "1" : "0";
                            break;
                        case "release":
                            message = canalAdmin.releaseInstance(destination) ? "1" : "0";
                            break;
                        case "restart":
                            message = canalAdmin.restartInstance(destination) ? "1" : "0";
                            break;
                        default:
                            byte[] errorBytes = AdminNettyUtils.errorPacket(301,
                                MessageFormatter.format("InstanceAdmin action={} is unknown", action).getMessage());
                            AdminNettyUtils.write(ctx.getChannel(), errorBytes);
                            break;
                    }
                    AdminNettyUtils.write(ctx.getChannel(), AdminNettyUtils.ackPacket(message));
                    break;
                case LOG:
                    LogAdmin logAdmin = LogAdmin.parseFrom(packet.getBody());
                    action = logAdmin.getAction();
                    destination = logAdmin.getDestination();
                    String type = logAdmin.getType();
                    String file = logAdmin.getFile();
                    int count = logAdmin.getCount();
                    switch (type) {
                        case "server":
                            if ("list".equalsIgnoreCase(action)) {
                                message = canalAdmin.listCanalLog();
                            } else {
                                message = canalAdmin.canalLog(count);
                            }
                            break;
                        case "instance":
                            if ("list".equalsIgnoreCase(action)) {
                                message = canalAdmin.listInstanceLog(destination);
                            } else {
                                message = canalAdmin.instanceLog(destination, file, count);
                            }
                            break;
                        default:
                            byte[] errorBytes = AdminNettyUtils.errorPacket(301,
                                MessageFormatter.format("LogAdmin type={} is unknown", type).getMessage());
                            AdminNettyUtils.write(ctx.getChannel(), errorBytes);
                            break;
                    }
                    AdminNettyUtils.write(ctx.getChannel(), AdminNettyUtils.ackPacket(message));
                    break;
                default:
                    byte[] errorBytes = AdminNettyUtils.errorPacket(300,
                        MessageFormatter.format("packet type={} is NOT supported!", packet.getType()).getMessage());
                    AdminNettyUtils.write(ctx.getChannel(), errorBytes);
                    break;
            }
        } catch (Throwable exception) {
            byte[] errorBytes = AdminNettyUtils.errorPacket(400,
                MessageFormatter.format("something goes wrong with channel:{}, exception={}",
                    ctx.getChannel(),
                    ExceptionUtils.getStackTrace(exception)).getMessage());
            AdminNettyUtils.write(ctx.getChannel(), errorBytes);
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.error("something goes wrong with channel:{}, exception={}",
            ctx.getChannel(),
            ExceptionUtils.getStackTrace(e.getCause()));

        ctx.getChannel().close();
    }

    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    }

    public void setCanalAdmin(CanalAdmin canalAdmin) {
        this.canalAdmin = canalAdmin;
    }

}

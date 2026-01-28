package com.alibaba.otter.canal.admin.handler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
        Packet packet = Packet.parseFrom(buffer.readBytes(buffer.readableBytes()).array());
        try {
            switch (packet.getType()) {
                case SERVER:
                    ServerAdmin sa = ServerAdmin.parseFrom(packet.getBody());
                    logger.info("received {} SERVER request in session handler...", sa.getAction());
                    byte[] svrResp = null;
                    switch (sa.getAction()) {
                        case "check":
                            svrResp = AdminNettyUtils.ackPacket(canalAdmin.check() ? "1" : "0");
                            break;
                        case "start":
                            svrResp = AdminNettyUtils.ackPacket(canalAdmin.start() ? "1" : "0");
                            break;
                        case "stop":
                            svrResp = AdminNettyUtils.ackPacket(canalAdmin.stop() ? "1" : "0");
                            break;
                        case "restart":
                            svrResp = AdminNettyUtils.ackPacket(canalAdmin.restart() ? "1" : "0");
                            break;
                        case "list":
                            svrResp = AdminNettyUtils.ackPacket(canalAdmin.getRunningInstances());
                            break;
                        default:
                            svrResp = AdminNettyUtils.errorPacket(301,
                                "ServerAdmin action: " + sa.getAction() + "  is unknown");
                            break;
                    }
                    AdminNettyUtils.write(ctx.getChannel(), svrResp);
                    break;

                case INSTANCE:
                    InstanceAdmin ia = InstanceAdmin.parseFrom(packet.getBody());
                    logger.info("received {} INSTANCE request in session handler...", ia.getAction());
                    byte[] instResp = null;
                    switch (ia.getAction()) {
                        case "check":
                            instResp = AdminNettyUtils
                                .ackPacket(canalAdmin.checkInstance(ia.getDestination()) ? "1" : "0");
                            break;
                        case "start":
                            instResp = AdminNettyUtils
                                .ackPacket(canalAdmin.startInstance(ia.getDestination()) ? "1" : "0");
                            break;
                        case "stop":
                            instResp = AdminNettyUtils
                                .ackPacket(canalAdmin.stopInstance(ia.getDestination()) ? "1" : "0");
                            break;
                        case "release":
                            instResp = AdminNettyUtils
                                .ackPacket(canalAdmin.releaseInstance(ia.getDestination()) ? "1" : "0");
                            break;
                        case "restart":
                            instResp = AdminNettyUtils
                                .ackPacket(canalAdmin.restartInstance(ia.getDestination()) ? "1" : "0");
                            break;
                        default:
                            instResp = AdminNettyUtils.errorPacket(301,
                                "InstanceAdmin action: " + ia.getAction() + " is unknown");
                            break;
                    }
                    AdminNettyUtils.write(ctx.getChannel(), instResp);
                    break;

                case LOG:
                    LogAdmin la = LogAdmin.parseFrom(packet.getBody());
                    logger.info("received {} LOG request in session handler...", la.getType());
                    byte[] logResp = null;
                    switch (la.getType()) {
                        case "server":
                            if ("list".equalsIgnoreCase(la.getAction())) {
                                logResp = AdminNettyUtils.ackPacket(canalAdmin.listCanalLog());
                            } else {
                                logResp = AdminNettyUtils.ackPacket(canalAdmin.canalLog(la.getCount()));
                            }
                            break;
                        case "instance":
                            if ("list".equalsIgnoreCase(la.getAction())) {
                                logResp = AdminNettyUtils.ackPacket(canalAdmin.listInstanceLog(la.getDestination()));
                            } else {
                                logResp = AdminNettyUtils.ackPacket(
                                    canalAdmin.instanceLog(la.getDestination(), la.getFile(), la.getCount()));
                            }
                            break;
                        default:
                            logResp = AdminNettyUtils.errorPacket(301,
                                "LogAdmin type: " + la.getType() + " is unknown");
                            break;
                    }
                    AdminNettyUtils.write(ctx.getChannel(), logResp);
                    break;

                default:
                    logResp = AdminNettyUtils.errorPacket(300,
                        "packet type: " + packet.getType() + " is NOT supported!");
                    AdminNettyUtils.write(ctx.getChannel(), logResp);
                    break;
            }

        } catch (Throwable exception) {
            String error = "something goes wrong with channel: " + ctx.getChannel() + ", exception: "
                           + ExceptionUtils.getStackTrace(exception);
            AdminNettyUtils.write(ctx.getChannel(), AdminNettyUtils.errorPacket(400, error));
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

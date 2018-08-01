package com.alibaba.otter.canal.server.netty.listener;

import com.alibaba.otter.canal.protocol.CanalPacket;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Chuanyi Li
 */
public class ChannelFutureAggregator implements ChannelFutureListener {

    private String                 dest;

    private CanalPacket.PacketType type;

    private int                    amount;

    private int                    errorCode;

    public ChannelFutureAggregator(String dest, CanalPacket.PacketType type, int amount) {
        this(dest, type, amount, 0);
    }

    public ChannelFutureAggregator(String dest, CanalPacket.PacketType type, int amount, int errorCode) {
        this.dest = dest;
        this.type = type;
        this.amount = amount;
        this.errorCode = errorCode;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
            ConcurrentHashMap map = new ConcurrentHashMap();
            map.putIfAbsent("", "");
        } else {
            Throwable t = future.getCause();
        }
    }
}

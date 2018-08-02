package com.alibaba.otter.canal.server.netty.listener;

import com.alibaba.otter.canal.protocol.CanalPacket;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.util.Optional;
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

    public static class ClientRequestResult {

        private String                 dest;

        private CanalPacket.PacketType type;

        private int                    amount;

        private int                    errorCode;

        private ClientRequestResult() {}

        private ClientRequestResult(Builder builder) {
            this.dest = builder.dest;
            this.type = builder.type;
            this.amount = builder.amount;
            this.errorCode = builder.errorCode;
        }

        private static class Builder {

            private String                 dest;
            private CanalPacket.PacketType type;
            private int                    amount;
            private int                    errorCode;

            public Builder dest(String dest) {
                this.dest = dest;
                return this;
            }

            public Builder type(CanalPacket.PacketType type) {
                this.type = type;
                return this;
            }

            public Builder amount(int amount) {
                this.amount = amount;
                return this;
            }

            public Builder errorCode(int errorCode) {
                this.errorCode = errorCode;
                return this;
            }

            public Builder fromPrototype(ClientRequestResult prototype) {
                dest = prototype.dest;
                type = prototype.type;
                amount = prototype.amount;
                errorCode = prototype.errorCode;
                return this;
            }

            public ClientRequestResult build() {
                return new ClientRequestResult(this);
            }
        }
    }
}

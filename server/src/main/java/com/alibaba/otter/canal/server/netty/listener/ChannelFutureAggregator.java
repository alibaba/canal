package com.alibaba.otter.canal.server.netty.listener;

import com.alibaba.otter.canal.protocol.CanalPacket;
import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessage;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import static com.alibaba.otter.canal.server.netty.CanalServerWithNettyProfiler.profiler;

/**
 * @author Chuanyi Li
 */
public class ChannelFutureAggregator implements ChannelFutureListener {

    private ClientRequestResult result;

    public ChannelFutureAggregator(String dest, GeneratedMessage request, CanalPacket.PacketType type, int amount, long latency) {
        this(dest, request, type, amount, latency, 0);
    }

    private ChannelFutureAggregator(String dest, GeneratedMessage request, CanalPacket.PacketType type, int amount, long latency, int errorCode) {
        this.result = new ClientRequestResult.Builder()
                .dest(dest)
                .type(type)
                .request(request)
                .amount(amount)
                .latency(latency)
                .errorCode(errorCode)
                .build();
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        // profiling after I/O operation
        if (future.getCause() != null) {
            result.channelError = future.getCause();
        }
        profiler().profiling(result.dest, result);
    }

    /**
     * Client request result pojo
     */
    public static class ClientRequestResult {

        private String                 dest;
        private CanalPacket.PacketType type;
        private GeneratedMessage       request;
        private int       amount;
        private long      latency;
        private int       errorCode;
        private Throwable channelError;

        private ClientRequestResult() {}

        private ClientRequestResult(Builder builder) {
            this.dest = Preconditions.checkNotNull(builder.dest);
            this.type = Preconditions.checkNotNull(builder.type);
            this.request = Preconditions.checkNotNull(builder.request);
            this.amount = builder.amount;
            this.latency = builder.latency;
            this.errorCode = builder.errorCode;
            this.channelError = builder.channelError;
        }

        // auto-generated
        public static class Builder {

            private String                 dest;
            private CanalPacket.PacketType type;
            private GeneratedMessage       request;
            private int                    amount;
            private long                   latency;
            private int                    errorCode;
            private Throwable              channelError;

            Builder dest(String dest) {
                this.dest = dest;
                return this;
            }

            Builder type(CanalPacket.PacketType type) {
                this.type = type;
                return this;
            }

            Builder request(GeneratedMessage request) {
                this.request = request;
                return this;
            }

            Builder amount(int amount) {
                this.amount = amount;
                return this;
            }

            Builder latency(long latency) {
                this.latency = latency;
                return this;
            }

            Builder errorCode(int errorCode) {
                this.errorCode = errorCode;
                return this;
            }

            public Builder channelError(Throwable channelError) {
                this.channelError = channelError;
                return this;
            }

            public Builder fromPrototype(ClientRequestResult prototype) {
                dest = prototype.dest;
                type = prototype.type;
                request = prototype.request;
                amount = prototype.amount;
                latency = prototype.latency;
                errorCode = prototype.errorCode;
                channelError = prototype.channelError;
                return this;
            }

            ClientRequestResult build() {
                return new ClientRequestResult(this);
            }
        }
        // getters
        public String getDest() {
            return dest;
        }

        public CanalPacket.PacketType getType() {
            return type;
        }

        public GeneratedMessage getRequest() {
            return request;
        }

        public int getAmount() {
            return amount;
        }

        public long getLatency() {
            return latency;
        }

        public int getErrorCode() {
            return errorCode;
        }

        public Throwable getChannelError() {
            return channelError;
        }
    }
}

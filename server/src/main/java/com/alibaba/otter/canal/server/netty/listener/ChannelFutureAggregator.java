package com.alibaba.otter.canal.server.netty.listener;

import com.alibaba.otter.canal.protocol.CanalPacket;
import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessageV3;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import static com.alibaba.otter.canal.server.netty.CanalServerWithNettyProfiler.profiler;
import static com.alibaba.otter.canal.server.netty.NettyUtils.HEADER_LENGTH;

/**
 * @author Chuanyi Li
 */
public class ChannelFutureAggregator implements ChannelFutureListener {

    private ClientRequestResult result;

    public ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency, boolean empty) {
        this(destination, request, type, amount, latency, empty, (short) 0);
    }

    public ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency) {
        this(destination, request, type, amount, latency, false, (short) 0);
    }

    public ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency, short errorCode) {
        this(destination, request, type, amount, latency, false, errorCode);
    }

    private ChannelFutureAggregator(String destination, GeneratedMessageV3 request, CanalPacket.PacketType type, int amount, long latency, boolean empty, short errorCode) {
        this.result = new ClientRequestResult.Builder()
                .destination(destination)
                .type(type)
                .request(request)
                .amount(amount + HEADER_LENGTH)
                .latency(latency)
                .errorCode(errorCode)
                .empty(empty)
                .build();
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        // profiling after I/O operation
        if (future != null && future.getCause() != null) {
            result.channelError = future.getCause();
        }
        profiler().profiling(result);
    }

    /**
     * Client request result pojo
     */
    public static class ClientRequestResult {

        private String                 destination;
        private CanalPacket.PacketType type;
        private GeneratedMessageV3       request;
        private int                    amount;
        private long                   latency;
        private short                  errorCode;
        private boolean                empty;
        private Throwable              channelError;

        private ClientRequestResult() {}

        private ClientRequestResult(Builder builder) {
            this.destination = Preconditions.checkNotNull(builder.destination);
            this.type = Preconditions.checkNotNull(builder.type);
            this.request = builder.request;
            this.amount = builder.amount;
            this.latency = builder.latency;
            this.errorCode = builder.errorCode;
            this.empty = builder.empty;
            this.channelError = builder.channelError;
        }

        // auto-generated
        public static class Builder {

            private String                 destination;
            private CanalPacket.PacketType type;
            private GeneratedMessageV3       request;
            private int                    amount;
            private long                   latency;
            private short                  errorCode;
            private boolean                empty;
            private Throwable              channelError;

            Builder destination(String destination) {
                this.destination = destination;
                return this;
            }

            Builder type(CanalPacket.PacketType type) {
                this.type = type;
                return this;
            }

            Builder request(GeneratedMessageV3 request) {
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

            Builder errorCode(short errorCode) {
                this.errorCode = errorCode;
                return this;
            }

            Builder empty(boolean empty) {
                this.empty = empty;
                return this;
            }

            public Builder channelError(Throwable channelError) {
                this.channelError = channelError;
                return this;
            }

            public Builder fromPrototype(ClientRequestResult prototype) {
                destination = prototype.destination;
                type = prototype.type;
                request = prototype.request;
                amount = prototype.amount;
                latency = prototype.latency;
                errorCode = prototype.errorCode;
                empty = prototype.empty;
                channelError = prototype.channelError;
                return this;
            }

            ClientRequestResult build() {
                return new ClientRequestResult(this);
            }
        }
        // getters
        public String getDestination() {
            return destination;
        }

        public CanalPacket.PacketType getType() {
            return type;
        }

        public GeneratedMessageV3 getRequest() {
            return request;
        }

        public int getAmount() {
            return amount;
        }

        public long getLatency() {
            return latency;
        }

        public short getErrorCode() {
            return errorCode;
        }

        public boolean getEmpty() {
            return empty;
        }

        public Throwable getChannelError() {
            return channelError;
        }
    }
}

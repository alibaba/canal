package com.alibaba.otter.canal.server.netty;

import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator;

/**
 * @author Chuanyi Li
 */
public interface ClientInstanceProfiler {

    String getDestination();

    void profiling(ChannelFutureAggregator.ClientRequestResult result);

}

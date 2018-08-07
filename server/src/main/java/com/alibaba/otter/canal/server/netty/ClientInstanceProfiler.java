package com.alibaba.otter.canal.server.netty;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator.ClientRequestResult;

/**
 * @author Chuanyi Li
 */
public interface ClientInstanceProfiler extends CanalLifeCycle {

    void profiling(ClientRequestResult result);

}

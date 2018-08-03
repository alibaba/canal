package com.alibaba.otter.canal.server.netty;

import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator.ClientRequestResult;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Chuanyi Li
 */
public class CanalServerWithNettyProfiler {

    private static final ClientInstanceProfilerFactory          DISABLED     = new DefaultClientInstanceProfilerFactory();

    private volatile ClientInstanceProfilerFactory              factory;

    private final ConcurrentMap<String, ClientInstanceProfiler> cliPfs;

    private final CanalServerWithEmbedded                       server;

    private static class SingletonHolder {
        private static CanalServerWithNettyProfiler SINGLETON = new CanalServerWithNettyProfiler();
    }

    private CanalServerWithNettyProfiler() {
        this.factory = DISABLED;
        this.cliPfs = new ConcurrentHashMap<String, ClientInstanceProfiler>();
        this.server = CanalServerWithEmbedded.instance();
    }

    public static CanalServerWithNettyProfiler profiler() {
        return SingletonHolder.SINGLETON;
    }

    public void profiling(String dest, ClientRequestResult result) {
        if (isDisabled()) {
            return;
        }
        ClientInstanceProfiler profiler = tryGet(dest);
        if (profiler != null) {
            profiler.profiling(result);
        }
    }

    /**
     * Remove instance profiler for specified instance.
     * Only accepted while instance is not running.
     * @param dest canal instance destination
     */
    public void remove(String dest) throws IllegalStateException {
        if (isDisabled()) {
            return;
        }
        synchronized (cliPfs) {
            if (server.isStart(dest)) {
                throw new IllegalStateException("Instance profiler should not be removed while running.");
            }
            cliPfs.remove(dest);
        }
    }

    public void setFactory(ClientInstanceProfilerFactory factory) {
        this.factory = factory;
    }

    private boolean isDisabled() {
        return factory == DISABLED || factory == null;
    }

    private ClientInstanceProfiler tryGet(String dest) {
        //try fast get
        ClientInstanceProfiler profiler = cliPfs.get(dest);
        if (profiler == null) {
            synchronized (cliPfs) {
                if (server.isStart(dest)) {
                    // avoid overwriting
                    cliPfs.putIfAbsent(dest, factory.create());
                    profiler = cliPfs.get(dest);
                }
            }
        }
        return profiler;
    }

    private static class DefaultClientInstanceProfilerFactory implements ClientInstanceProfilerFactory {
        @Override
        public ClientInstanceProfiler create() {
            throw new UnsupportedOperationException();
        }
    }

}

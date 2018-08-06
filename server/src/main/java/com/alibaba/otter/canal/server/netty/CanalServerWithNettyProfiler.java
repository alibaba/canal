package com.alibaba.otter.canal.server.netty;

import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.listener.ChannelFutureAggregator.ClientRequestResult;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Chuanyi Li
 */
public class CanalServerWithNettyProfiler {

    public static final ClientInstanceProfilerFactory           DISABLED = new DefaultClientInstanceProfilerFactory();
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

    public void profiling(String destination, ClientRequestResult result) {
        if (isDisabled()) {
            return;
        }
        ClientInstanceProfiler profiler = cliPfs.get(destination);
        if (profiler != null) {
            profiler.profiling(result);
        }
    }

    public void start(String destination) {
        if (isDisabled()) {
            return;
        }
        if (server.isStart(destination)) {
            throw new IllegalStateException("Instance profiler should not be start while running.");
        }
        ClientInstanceProfiler profiler = factory.create(destination);
        profiler.start();
        cliPfs.put(destination, profiler);
    }

    /**
     * Remove instance profiler for specified instance.
     * Only accepted while instance is not running.
     * @param destination canal instance destination
     */
    public void stop(String destination) {
        if (isDisabled()) {
            return;
        }
        if (server.isStart(destination)) {
            throw new IllegalStateException("Instance profiler should not be stop while running.");
        }
        ClientInstanceProfiler profiler = cliPfs.remove(destination);
        if (profiler != null && profiler.isStart()) {
            profiler.stop();
        }
    }

    public void setInstanceProfilerFactory(ClientInstanceProfilerFactory factory) {
        this.factory = factory;
    }

    private boolean isDisabled() {
        return factory == DISABLED || factory == null;
    }

    private ClientInstanceProfiler tryGet(String destination) {
        //try fast get
        ClientInstanceProfiler profiler = cliPfs.get(destination);
        if (profiler == null) {
            synchronized (cliPfs) {
                if (server.isStart(destination)) {
                    // avoid overwriting
                    cliPfs.putIfAbsent(destination, factory.create(destination));
                    profiler = cliPfs.get(destination);
                    if (!profiler.isStart()) {
                        profiler.start();
                    }
                }
            }
        }
        return profiler;
    }

    private static class DefaultClientInstanceProfilerFactory implements ClientInstanceProfilerFactory {
        @Override
        public ClientInstanceProfiler create(String destination) {
            throw new UnsupportedOperationException();
        }
    }

}

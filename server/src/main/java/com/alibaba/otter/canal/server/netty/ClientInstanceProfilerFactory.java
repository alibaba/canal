package com.alibaba.otter.canal.server.netty;

/**
 * @author Chuanyi Li
 */
public interface ClientInstanceProfilerFactory {

    ClientInstanceProfiler create(String destination);

}

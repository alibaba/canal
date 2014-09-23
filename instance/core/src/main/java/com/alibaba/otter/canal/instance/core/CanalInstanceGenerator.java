package com.alibaba.otter.canal.instance.core;

/**
 * @author zebin.xuzb @ 2012-7-12
 * @version 1.0.0
 */
public interface CanalInstanceGenerator {

    /**
     * 通过 destination 产生特定的 {@link CanalInstance}
     * 
     * @param destination
     * @return
     */
    CanalInstance generate(String destination);
}

package com.alibaba.otter.canal.prometheus;

import com.alibaba.otter.canal.instance.core.CanalInstance;

/**
 * @author Chuanyi Li
 */
public interface InstanceRegistry {

    void register(CanalInstance instance);

    void unregister(CanalInstance instance);

}

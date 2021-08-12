package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * @author wanghe
 */
public interface MultiStageCoprocessor extends CanalLifeCycle {

    /**
     * publish data to the coprocessor
     *
     * @param data message to deal with in processor
     * @return flag whether coprocessor is working properly
     */
    boolean publish(Object data);
}

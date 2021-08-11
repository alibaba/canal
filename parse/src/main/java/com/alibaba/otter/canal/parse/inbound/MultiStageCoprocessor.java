package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.CanalLifeCycle;

public interface MultiStageCoprocessor extends CanalLifeCycle {

    boolean publish(Object data);
}

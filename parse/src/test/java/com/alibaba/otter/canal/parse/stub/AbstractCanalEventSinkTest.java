package com.alibaba.otter.canal.parse.stub;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.sink.CanalEventSink;

public abstract class AbstractCanalEventSinkTest<T> extends AbstractCanalLifeCycle implements CanalEventSink<T> {

    public void interrupt() {
        // do nothing
    }
}

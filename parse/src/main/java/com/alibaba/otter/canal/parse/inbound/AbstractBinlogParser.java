package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;

public abstract class AbstractBinlogParser<T> extends AbstractCanalLifeCycle implements BinlogParser<T> {

    public void reset() {

    }

    public void stop() {
        reset();
        super.stop();
    }

}

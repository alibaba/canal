package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;

public abstract class AbstractBinlogParser extends AbstractCanalLifeCycle implements BinlogParser {

    public void reset() {

    }

    public void stop() {
        reset();
        super.stop();
    }

}

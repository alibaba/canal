package com.alibaba.otter.canal.parse.inbound;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

public abstract class AbstractBinlogParser<T> extends AbstractCanalLifeCycle implements BinlogParser<T> {

    public void reset() {
    }

    public Entry parse(T event, TableMeta tableMeta) throws CanalParseException {
        return null;
    }

    public Entry parse(T event) throws CanalParseException {
        return null;
    }

    public void stop() {
        reset();
        super.stop();
    }

}

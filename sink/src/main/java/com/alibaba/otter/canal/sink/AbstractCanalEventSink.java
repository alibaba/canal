package com.alibaba.otter.canal.sink;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;

/**
 * @author jianghang 2012-7-23 下午01:02:45
 */
public abstract class AbstractCanalEventSink<T> extends AbstractCanalLifeCycle implements CanalEventSink<T> {

    protected CanalEventFilter            filter;
    protected CanalEventDownStreamHandler handler;

    public void setFilter(CanalEventFilter filter) {
        this.filter = filter;
    }

    public void setHandler(CanalEventDownStreamHandler handler) {
        this.handler = handler;
    }

    public CanalEventFilter getFilter() {
        return filter;
    }

    public CanalEventDownStreamHandler getHandler() {
        return handler;
    }

    public void interrupt() {
        // do nothing
    }

}

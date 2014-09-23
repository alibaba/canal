package com.alibaba.otter.canal.sink;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;

/**
 * 默认的实现
 * 
 * @author jianghang 2013-10-8 下午8:35:29
 * @since 1.0.12
 */
public class AbstractCanalEventDownStreamHandler<T> extends AbstractCanalLifeCycle implements CanalEventDownStreamHandler<T> {

    public T before(T events) {
        return events;
    }

    public T retry(T events) {
        return events;
    }

    public T after(T events) {
        return events;
    }

}

package com.alibaba.otter.canal.sink;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 处理下sink时的数据流
 * 
 * @author jianghang 2012-7-31 下午03:06:26
 * @version 1.0.0
 */
public interface CanalEventDownStreamHandler<T> extends CanalLifeCycle {

    /**
     * 提交到store之前做一下处理，允许替换Event
     */
    public T before(T events);

    /**
     * store处于full后，retry时处理做一下处理
     */
    public T retry(T events);

    /**
     * 提交store成功后做一下处理
     */
    public T after(T events);
}

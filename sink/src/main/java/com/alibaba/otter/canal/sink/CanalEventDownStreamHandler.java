package com.alibaba.otter.canal.sink;

import java.util.List;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 处理下sink时的数据流
 * 
 * @author jianghang 2012-7-31 下午03:06:26
 * @version 4.1.0
 */
public interface CanalEventDownStreamHandler<T> extends CanalLifeCycle {

    /**
     * 提交到store之前做一下处理
     */
    public void before(List<T> event);

    /**
     * store处于full后，retry时处理做一下处理
     */
    public void retry(List<T> event);

    /**
     * 提交store成功后做一下处理
     */
    public void after(List<T> event);
}

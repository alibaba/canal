package com.alibaba.otter.canal.sink;

import com.alibaba.otter.canal.sink.exception.CanalSinkException;

/**
 * 数据过滤机制
 * 
 * @author jianghang 2012-7-20 下午03:51:27
 */
public interface CanalEventFilter<T> {

    boolean filter(T event) throws CanalSinkException;
}

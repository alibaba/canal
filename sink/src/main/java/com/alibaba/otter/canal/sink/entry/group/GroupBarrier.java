package com.alibaba.otter.canal.sink.entry.group;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 针对group合并的barrier接口，控制多个sink操作的合并处理
 * 
 * @author jianghang 2012-10-18 下午05:07:35
 * @version 1.0.0
 */
public interface GroupBarrier<T> {

    /**
     * 判断当前的数据对象是否允许通过
     * 
     * @param event
     * @throws InterruptedException
     */
    public void await(T event) throws InterruptedException;

    /**
     * 判断当前的数据对象是否允许通过，带超时控制
     * 
     * @param event
     * @param timeout
     * @param unit
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void await(T event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * sink成功，清理对应barrier的状态
     */
    public void clear(T event);

    /**
     * 出现切换，发起interrupt，清理对应的上下文
     */
    public void interrupt();
}

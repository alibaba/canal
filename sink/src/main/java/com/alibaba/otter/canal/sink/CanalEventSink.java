package com.alibaba.otter.canal.sink;

import java.net.InetSocketAddress;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.sink.entry.group.GroupEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;

/**
 * event事件消费者
 * 
 * <pre>
 * 1. 剥离filter/sink为独立的两个动作，方便在快速判断数据是否有效
 * </pre>
 * 
 * @author jianghang 2012-6-21 下午05:03:40
 * @version 1.0.0
 */
public interface CanalEventSink<T> extends CanalLifeCycle {

    /**
     * 提交数据
     * 
     * @param event
     * @param remoteAddress
     * @param destination
     * @throws CanalSinkException
     * @throws InterruptedException
     */
    boolean sink(T event, InetSocketAddress remoteAddress, String destination) throws CanalSinkException,
                                                                              InterruptedException;

    /**
     * 中断消费，比如解析模块发生了切换，想临时中断当前的merge请求，清理对应的上下文状态，可见{@linkplain GroupEventSink}
     */
    void interrupt();

}

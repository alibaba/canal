package com.alibaba.otter.canal.instance.core;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

/**
 * 代表单个canal实例，比如一个destination会独立一个实例
 * 
 * @author jianghang 2012-7-12 下午12:04:58
 * @version 1.0.0
 */
public interface CanalInstance extends CanalLifeCycle {

    public String getDestination();

    public CanalEventParser getEventParser();

    public CanalEventSink getEventSink();

    public CanalEventStore getEventStore();

    public CanalMetaManager getMetaManager();

    public CanalAlarmHandler getAlarmHandler();

    /**
     * 客户端发生订阅/取消订阅行为
     */
    public boolean subscribeChange(ClientIdentity identity);
}

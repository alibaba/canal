package com.alibaba.otter.canal.instance.spring;

import java.util.List;

import com.alibaba.erosa.protocol.protobuf.ErosaEntry.Entry;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

/**
 * 基于spring容器启动canal实例，方便独立于manager启动
 * 
 * @author jianghang 2012-7-12 下午01:21:26
 * @author zebin.xuzb
 * @version 4.1.0
 */
public class CanalInstanceWithSpring extends AbstractCanalLifeCycle implements CanalInstance {

    private String                      destination;
    private CanalEventParser            eventParser;
    private CanalEventSink<List<Entry>> eventSink;
    private CanalEventStore<Entry>      eventStore;
    private CanalHAController           haController;
    private CanalLogPositionManager     logPositionManager;
    private CanalMetaManager            metaManager;
    private CanalAlarmHandler           alarmHandler;

    public String getDestination() {
        return this.destination;
    }

    public CanalEventParser getEventParser() {
        return this.eventParser;
    }

    public CanalEventSink<List<Entry>> getEventSink() {
        return this.eventSink;
    }

    public CanalEventStore<Entry> getEventStore() {
        return this.eventStore;
    }

    public CanalHAController getHaController() {
        return this.haController;
    }

    public CanalLogPositionManager getLogPositionManager() {
        return this.logPositionManager;
    }

    public CanalMetaManager getMetaManager() {
        return this.metaManager;
    }

    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    // ======== setter ========
    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setEventParser(CanalEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setEventSink(CanalEventSink<List<Entry>> eventSink) {
        this.eventSink = eventSink;
    }

    public void setEventStore(CanalEventStore<Entry> eventStore) {
        this.eventStore = eventStore;
    }

    public void setHaController(CanalHAController haController) {
        this.haController = haController;
    }

    public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
        this.logPositionManager = logPositionManager;
    }

    public void setMetaManager(CanalMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

}

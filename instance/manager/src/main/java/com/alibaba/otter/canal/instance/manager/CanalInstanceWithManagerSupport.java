package com.alibaba.otter.canal.instance.manager;

import java.util.List;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * @author zebin.xuzb 2012-10-17 下午3:12:34
 * @version 1.0.0
 */
public abstract class CanalInstanceWithManagerSupport extends AbstractCanalLifeCycle {

    // around alarm handler start
    protected void beforeStartAlarmHandler(CanalAlarmHandler alarmHandler) {
        // noop
    }

    protected void afterStartAlarmHandler(CanalAlarmHandler alarmHandler) {
        // noop
    }

    // around meta manager start
    protected void beforeStartMetaManager(CanalMetaManager metaManager) {
        // noop
    }

    protected void afterStartMetaManager(CanalMetaManager metaManager) {
        // noop
    }

    // around event store
    protected void beforeStartEventStore(CanalEventStore<Event> eventStore) {
        // noop
    }

    protected void afterStartEventStore(CanalEventStore<Event> eventStore) {
        // noop
    }

    // around event sink
    protected void beforeStartEventSink(CanalEventSink<List<Entry>> eventSink) {
        // noop
    }

    protected void afterStartEventSink(CanalEventSink<List<Entry>> eventSink) {
        // noop
    }

    // around log position manager
    protected void beforeStartLogPositionManager(CanalLogPositionManager logPositionManager) {
        // noop
    }

    protected void afterStartLogPositionManager(CanalLogPositionManager logPositionManager) {
        // noop
    }

    // around event parser
    protected void beforeStartEventParser(CanalEventParser eventParser) {
        // noop
    }

    protected void afterStartEventParser(CanalEventParser eventParser) {
        // noop
    }

    // around HA controller
    protected void beforeStartHAController(CanalHAController haController) {
        // noop
    }

    protected void afterStartHAController(CanalHAController haController) {
        // noop
    }

}

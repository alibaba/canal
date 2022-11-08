package com.alibaba.otter.canal.instance.spring;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.instance.core.AbstractCanalInstance;
import com.alibaba.otter.canal.instance.core.CanalMQConfig;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 基于spring容器启动canal实例，方便独立于manager启动
 * 
 * @author jianghang 2012-7-12 下午01:21:26
 * @author zebin.xuzb
 * @version 1.0.0
 */
public class CanalInstanceWithSpring extends AbstractCanalInstance {

    private static final Logger logger = LoggerFactory.getLogger(CanalInstanceWithSpring.class);

    public void start() {
        logger.info("start CannalInstance for {}-{} ", new Object[] { 1, destination });
        super.start();
    }

    // ======== setter ========

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setEventParser(CanalEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setEventSink(CanalEventSink<List<CanalEntry.Entry>> eventSink) {
        this.eventSink = eventSink;
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public void setMetaManager(CanalMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

    public void setMqConfig(CanalMQConfig mqConfig) {
        this.mqConfig = mqConfig;
    }

}

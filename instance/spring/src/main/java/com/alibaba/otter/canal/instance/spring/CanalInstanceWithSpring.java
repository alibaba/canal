package com.alibaba.otter.canal.instance.spring;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceSupport;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
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
public class CanalInstanceWithSpring extends CanalInstanceSupport implements CanalInstance {

    private static final Logger                    logger = LoggerFactory.getLogger(CanalInstanceWithSpring.class);
    private String                                 destination;
    private CanalEventParser                       eventParser;
    private CanalEventSink<List<CanalEntry.Entry>> eventSink;
    private CanalEventStore<Event>                 eventStore;
    private CanalMetaManager                       metaManager;
    private CanalAlarmHandler                      alarmHandler;
    private CanalHAController                      haController;
    private CanalLogPositionManager                logPositionManager;

    public String getDestination() {
        return this.destination;
    }

    public CanalEventParser getEventParser() {
        return this.eventParser;
    }

    public CanalEventSink<List<CanalEntry.Entry>> getEventSink() {
        return this.eventSink;
    }

    public CanalEventStore<Event> getEventStore() {
        return this.eventStore;
    }

    public CanalMetaManager getMetaManager() {
        return this.metaManager;
    }

    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    public boolean subscribeChange(ClientIdentity identity) {
        if (StringUtils.isNotEmpty(identity.getFilter())) {
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());
            ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
        }

        // filter的处理规则
        // a. parser处理数据过滤处理
        // b. sink处理数据的路由&分发,一份parse数据经过sink后可以分发为多份，每份的数据可以根据自己的过滤规则不同而有不同的数据
        // 后续内存版的一对多分发，可以考虑
        return true;
    }

    public void start() {
        super.start();

        logger.info("start CannalInstance for {}-{} ", new Object[] { 1, destination });
        if (!metaManager.isStart()) {
            beforeStartMetaManager(metaManager);
            metaManager.start();
            afterStartMetaManager(metaManager);
        }

        if (!eventStore.isStart()) {
            beforeStartEventStore(eventStore);
            eventStore.start();
            afterStartEventStore(eventStore);
        }

        if (!eventSink.isStart()) {
            beforeStartEventSink(eventSink);
            eventSink.start();
            afterStartEventSink(eventSink);
        }

        if (!logPositionManager.isStart()) {
            beforeStartLogPositionManager(logPositionManager);
            logPositionManager.start();
            afterStartLogPositionManager(logPositionManager);
        }

        if (!haController.isStart()) {
            beforeStartHAController(haController);
            haController.start();
            afterStartHAController(haController);
        }

        if (!eventParser.isStart()) {
            beforeStartEventParser(eventParser);
            eventParser.start();
            afterStartEventParser(eventParser);
        }

        logger.info("start successful....");
    }

    public void stop() {
        logger.info("stop CannalInstance for {}-{} ", new Object[] { 1, destination });
        if (eventParser.isStart()) {
            eventParser.stop();
        }

        if (haController.isStart()) {
            haController.stop();
        }

        if (logPositionManager.isStart()) {
            logPositionManager.stop();
        }

        if (eventSink.isStart()) {
            eventSink.stop();
        }

        if (eventStore.isStart()) {
            eventStore.stop();
        }

        if (metaManager.isStart()) {
            metaManager.stop();
        }

        if (alarmHandler.isStart()) {
            alarmHandler.stop();
        }

        // if (zkClientx != null) {
        // zkClientx.close();
        // }

        super.stop();
        logger.info("stop successful....");
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

    public CanalHAController getHaController() {
        return haController;
    }

    public void setHaController(CanalHAController haController) {
        this.haController = haController;
    }

    public CanalLogPositionManager getLogPositionManager() {
        return logPositionManager;
    }

    public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
        this.logPositionManager = logPositionManager;
    }

}

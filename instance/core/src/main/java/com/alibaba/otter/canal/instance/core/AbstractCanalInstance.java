package com.alibaba.otter.canal.instance.core;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * Created with Intellig IDEA. Author: yinxiu Date: 2016-01-07 Time: 22:26
 */
public class AbstractCanalInstance extends AbstractCanalLifeCycle implements CanalInstance {

    private static final Logger                      logger = LoggerFactory.getLogger(AbstractCanalInstance.class);

    protected Long                                   canalId;                                                      // 和manager交互唯一标示
    protected String                                 destination;                                                  // 队列名字
    protected CanalEventStore<Event>                 eventStore;                                                   // 有序队列

    protected CanalEventParser                       eventParser;                                                  // 解析对应的数据信息
    protected CanalEventSink<List<CanalEntry.Entry>> eventSink;                                                    // 链接parse和store的桥接器
    protected CanalMetaManager                       metaManager;                                                  // 消费信息管理器
    protected CanalAlarmHandler                      alarmHandler;                                                 // alarm报警机制
    protected CanalMQConfig                          mqConfig;                                                     // mq的配置



    @Override
    public boolean subscribeChange(ClientIdentity identity) {
        if (StringUtils.isNotEmpty(identity.getFilter())) {
            logger.info("subscribe filter change to " + identity.getFilter());
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());

            boolean isGroup = (eventParser instanceof GroupEventParser);
            if (isGroup) {
                // 处理group的模式
                List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
                for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                    if(singleEventParser instanceof AbstractEventParser) {
                        ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
                    }
                }
            } else {
                if(eventParser instanceof AbstractEventParser) {
                    ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
                }
            }

        }

        // filter的处理规则
        // a. parser处理数据过滤处理
        // b. sink处理数据的路由&分发,一份parse数据经过sink后可以分发为多份，每份的数据可以根据自己的过滤规则不同而有不同的数据
        // 后续内存版的一对多分发，可以考虑
        return true;
    }

    @Override
    public void start() {
        super.start();
        if (!metaManager.isStart()) {
            metaManager.start();
        }

        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }

        if (!eventStore.isStart()) {
            eventStore.start();
        }

        if (!eventSink.isStart()) {
            eventSink.start();
        }

        if (!eventParser.isStart()) {
            beforeStartEventParser(eventParser);
            eventParser.start();
            afterStartEventParser(eventParser);
        }
        logger.info("start successful....");
    }

    @Override
    public void stop() {
        super.stop();
        logger.info("stop CannalInstance for {}-{} ", new Object[] { canalId, destination });

        if (eventParser.isStart()) {
            beforeStopEventParser(eventParser);
            eventParser.stop();
            afterStopEventParser(eventParser);
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

        logger.info("stop successful....");
    }

    protected void beforeStartEventParser(CanalEventParser eventParser) {

        boolean isGroup = (eventParser instanceof GroupEventParser);
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                startEventParserInternal(singleEventParser, true);
            }
        } else {
            startEventParserInternal(eventParser, false);
        }
    }

    // around event parser, default impl
    protected void afterStartEventParser(CanalEventParser eventParser) {
        // 读取一下历史订阅的filter信息
        List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
        for (ClientIdentity clientIdentity : clientIdentitys) {
            subscribeChange(clientIdentity);
        }
    }

    // around event parser
    protected void beforeStopEventParser(CanalEventParser eventParser) {
        // noop
    }

    protected void afterStopEventParser(CanalEventParser eventParser) {

        boolean isGroup = (eventParser instanceof GroupEventParser);
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                stopEventParserInternal(singleEventParser);
            }
        } else {
            stopEventParserInternal(eventParser);
        }
    }

    /**
     * 初始化单个eventParser，不需要考虑group
     */
    protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (!logPositionManager.isStart()) {
                logPositionManager.start();
            }
        }

        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();

            if (haController instanceof HeartBeatHAController) {
                ((HeartBeatHAController) haController).setCanalHASwitchable(mysqlEventParser);
            }

            if (!haController.isStart()) {
                haController.start();
            }

        }
    }

    protected void stopEventParserInternal(CanalEventParser eventParser) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (logPositionManager.isStart()) {
                logPositionManager.stop();
            }
        }

        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();
            if (haController.isStart()) {
                haController.stop();
            }
        }
    }

    // ==================getter==================================
    @Override
    public String getDestination() {
        return destination;
    }

    @Override
    public CanalEventParser getEventParser() {
        return eventParser;
    }

    @Override
    public CanalEventSink getEventSink() {
        return eventSink;
    }

    @Override
    public CanalEventStore getEventStore() {
        return eventStore;
    }

    @Override
    public CanalMetaManager getMetaManager() {
        return metaManager;
    }

    @Override
    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    @Override
    public CanalMQConfig getMqConfig() {
        return mqConfig;
    }
}

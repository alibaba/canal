package com.alibaba.otter.canal.instance.manager;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.common.alarm.LogAlarmHandler;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceSupport;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.DataSourcing;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.HAMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.IndexMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.MetaMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.SourcingType;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.StorageMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.StorageScavengeMode;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.meta.MemoryMetaManager;
import com.alibaba.otter.canal.meta.PeriodMixedMetaManager;
import com.alibaba.otter.canal.meta.ZooKeeperMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinlogEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.index.FailbackLogPositionManager;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.parse.index.MetaLogPositionManager;
import com.alibaba.otter.canal.parse.index.PeriodMixedLogPositionManager;
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.sink.entry.group.GroupEventSink;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 单个canal实例，比如一个destination会独立一个实例
 * 
 * @author jianghang 2012-7-11 下午09:26:51
 * @version 1.0.0
 */
public class CanalInstanceWithManager extends CanalInstanceSupport implements CanalInstance {

    private static final Logger           logger = LoggerFactory.getLogger(CanalInstanceWithManager.class);
    protected Long                        canalId;                                                         // 和manager交互唯一标示
    protected String                      destination;                                                     // 队列名字
    protected String                      filter;                                                          // 过滤表达式
    protected CanalParameter              parameters;                                                      // 对应参数
    protected CanalMetaManager            metaManager;                                                     // 消费信息管理器
    protected CanalEventStore<Event>      eventStore;                                                      // 有序队列

    protected CanalEventParser            eventParser;                                                     // 解析对应的数据信息
    protected CanalEventSink<List<Entry>> eventSink;                                                       // 链接parse和store的桥接器
    protected CanalAlarmHandler           alarmHandler;                                                    // alarm报警机制
    protected ZkClientx                   zkClientx;

    public CanalInstanceWithManager(Canal canal){
        this(canal, null);
    }

    public CanalInstanceWithManager(Canal canal, String filter){
        this.parameters = canal.getCanalParameter();
        this.canalId = canal.getId();
        this.destination = canal.getName();
        this.filter = filter;

        logger.info("init CannalInstance for {}-{} with parameters:{}",
            new Object[] { canalId, destination, parameters });
        // 初始化报警机制
        initAlarmHandler();
        // 初始化metaManager
        initMetaManager();
        // 初始化eventStore
        initEventStore();
        // 初始化eventSink
        initEventSink();
        // 初始化eventParser;
        initEventParser();

        // 基础工具，需要提前start，会有先订阅再根据filter条件启动paser的需求
        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }

        if (!metaManager.isStart()) {
            metaManager.start();
        }
        logger.info("init successful....");
    }

    public void start() {
        super.start();
        // 初始化metaManager
        logger.info("start CannalInstance for {}-{} with parameters:{}", new Object[] { canalId, destination,
                parameters });

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
        }

        logger.info("start successful....");
    }

    public void stop() {
        logger.info("stop CannalInstance for {}-{} ", new Object[] { canalId, destination });

        if (eventParser.isStart()) {
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

        // if (zkClientx != null) {
        // zkClientx.close();
        // }

        super.stop();
        logger.info("stop successful....");
    }

    public boolean subscribeChange(ClientIdentity identity) {
        if (StringUtils.isNotEmpty(identity.getFilter())) {
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());

            boolean isGroup = (eventParser instanceof GroupEventParser);
            if (isGroup) {
                // 处理group的模式
                List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
                for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                    ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
                }
            } else {
                ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
            }

        }

        // filter的处理规则
        // a. parser处理数据过滤处理
        // b. sink处理数据的路由&分发,一份parse数据经过sink后可以分发为多份，每份的数据可以根据自己的过滤规则不同而有不同的数据
        // 后续内存版的一对多分发，可以考虑
        return true;
    }

    protected void afterStartEventParser(CanalEventParser eventParser) {
        super.afterStartEventParser(eventParser);

        // 读取一下历史订阅的filter信息
        List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
        for (ClientIdentity clientIdentity : clientIdentitys) {
            subscribeChange(clientIdentity);
        }
    }

    protected void initAlarmHandler() {
        logger.info("init alarmHandler begin...");
        alarmHandler = new LogAlarmHandler();
        logger.info("init alarmHandler end! \n\t load CanalAlarmHandler:{} ", alarmHandler.getClass().getName());
    }

    protected void initMetaManager() {
        logger.info("init metaManager begin...");
        MetaMode mode = parameters.getMetaMode();
        if (mode.isMemory()) {
            metaManager = new MemoryMetaManager();
        } else if (mode.isZookeeper()) {
            metaManager = new ZooKeeperMetaManager();
            ((ZooKeeperMetaManager) metaManager).setZkClientx(getZkclientx());
        } else if (mode.isMixed()) {
            // metaManager = new MixedMetaManager();
            metaManager = new PeriodMixedMetaManager();// 换用优化过的mixed, at
                                                       // 2012-09-11
            // 设置内嵌的zk metaManager
            ZooKeeperMetaManager zooKeeperMetaManager = new ZooKeeperMetaManager();
            zooKeeperMetaManager.setZkClientx(getZkclientx());
            ((PeriodMixedMetaManager) metaManager).setZooKeeperMetaManager(zooKeeperMetaManager);
        } else {
            throw new CanalException("unsupport MetaMode for " + mode);
        }

        logger.info("init metaManager end! \n\t load CanalMetaManager:{} ", metaManager.getClass().getName());
    }

    protected void initEventStore() {
        logger.info("init eventStore begin...");
        StorageMode mode = parameters.getStorageMode();
        if (mode.isMemory()) {
            MemoryEventStoreWithBuffer memoryEventStore = new MemoryEventStoreWithBuffer();
            memoryEventStore.setBufferSize(parameters.getMemoryStorageBufferSize());
            memoryEventStore.setBufferMemUnit(parameters.getMemoryStorageBufferMemUnit());
            memoryEventStore.setBatchMode(BatchMode.valueOf(parameters.getStorageBatchMode().name()));
            memoryEventStore.setDdlIsolation(parameters.getDdlIsolation());
            eventStore = memoryEventStore;
        } else if (mode.isFile()) {
            // 后续版本支持
            throw new CanalException("unsupport MetaMode for " + mode);
        } else if (mode.isMixed()) {
            // 后续版本支持
            throw new CanalException("unsupport MetaMode for " + mode);
        } else {
            throw new CanalException("unsupport MetaMode for " + mode);
        }

        if (eventStore instanceof AbstractCanalStoreScavenge) {
            StorageScavengeMode scavengeMode = parameters.getStorageScavengeMode();
            AbstractCanalStoreScavenge eventScavengeStore = (AbstractCanalStoreScavenge) eventStore;
            eventScavengeStore.setDestination(destination);
            eventScavengeStore.setCanalMetaManager(metaManager);
            eventScavengeStore.setOnAck(scavengeMode.isOnAck());
            eventScavengeStore.setOnFull(scavengeMode.isOnFull());
            eventScavengeStore.setOnSchedule(scavengeMode.isOnSchedule());
            if (scavengeMode.isOnSchedule()) {
                eventScavengeStore.setScavengeSchedule(parameters.getScavengeSchdule());
            }
        }
        logger.info("init eventStore end! \n\t load CanalEventStore:{}", eventStore.getClass().getName());
    }

    protected void initEventSink() {
        logger.info("init eventSink begin...");

        int groupSize = getGroupSize();
        if (groupSize <= 1) {
            eventSink = new EntryEventSink();
        } else {
            eventSink = new GroupEventSink(groupSize);
        }

        if (eventSink instanceof EntryEventSink) {
            ((EntryEventSink) eventSink).setFilterTransactionEntry(false);
            ((EntryEventSink) eventSink).setEventStore(getEventStore());
        }
        // if (StringUtils.isNotEmpty(filter)) {
        // AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(filter);
        // ((AbstractCanalEventSink) eventSink).setFilter(aviaterFilter);
        // }
        logger.info("init eventSink end! \n\t load CanalEventSink:{}", eventSink.getClass().getName());
    }

    protected void initEventParser() {
        logger.info("init eventParser begin...");
        SourcingType type = parameters.getSourcingType();

        List<List<DataSourcing>> groupDbAddresses = parameters.getGroupDbAddresses();
        if (!CollectionUtils.isEmpty(groupDbAddresses)) {
            int size = groupDbAddresses.get(0).size();// 取第一个分组的数量，主备分组的数量必须一致
            List<CanalEventParser> eventParsers = new ArrayList<CanalEventParser>();
            for (int i = 0; i < size; i++) {
                List<InetSocketAddress> dbAddress = new ArrayList<InetSocketAddress>();
                SourcingType lastType = null;
                for (List<DataSourcing> groupDbAddress : groupDbAddresses) {
                    if (lastType != null && !lastType.equals(groupDbAddress.get(i).getType())) {
                        throw new CanalException(String.format("master/slave Sourcing type is unmatch. %s vs %s",
                            lastType,
                            groupDbAddress.get(i).getType()));
                    }

                    lastType = groupDbAddress.get(i).getType();
                    dbAddress.add(groupDbAddress.get(i).getDbAddress());
                }

                // 初始化其中的一个分组parser
                eventParsers.add(doInitEventParser(lastType, dbAddress));
            }

            if (eventParsers.size() > 1) { // 如果存在分组，构造分组的parser
                GroupEventParser groupEventParser = new GroupEventParser();
                groupEventParser.setEventParsers(eventParsers);
                this.eventParser = groupEventParser;
            } else {
                this.eventParser = eventParsers.get(0);
            }
        } else {
            // 创建一个空数据库地址的parser，可能使用了tddl指定地址，启动的时候才会从tddl获取地址
            this.eventParser = doInitEventParser(type, new ArrayList<InetSocketAddress>());
        }

        logger.info("init eventParser end! \n\t load CanalEventParser:{}", eventParser.getClass().getName());
    }

    private CanalEventParser doInitEventParser(SourcingType type, List<InetSocketAddress> dbAddresses) {
        CanalEventParser eventParser = null;
        if (type.isMysql()) {
            MysqlEventParser mysqlEventParser = new MysqlEventParser();
            mysqlEventParser.setDestination(destination);
            // 编码参数
            mysqlEventParser.setConnectionCharset(Charset.forName(parameters.getConnectionCharset()));
            mysqlEventParser.setConnectionCharsetNumber(parameters.getConnectionCharsetNumber());
            // 网络相关参数
            mysqlEventParser.setDefaultConnectionTimeoutInSeconds(parameters.getDefaultConnectionTimeoutInSeconds());
            mysqlEventParser.setSendBufferSize(parameters.getSendBufferSize());
            mysqlEventParser.setReceiveBufferSize(parameters.getReceiveBufferSize());
            // 心跳检查参数
            mysqlEventParser.setDetectingEnable(parameters.getDetectingEnable());
            mysqlEventParser.setDetectingSQL(parameters.getDetectingSQL());
            mysqlEventParser.setDetectingIntervalInSeconds(parameters.getDetectingIntervalInSeconds());
            // 数据库信息参数
            mysqlEventParser.setSlaveId(parameters.getSlaveId());
            if (!CollectionUtils.isEmpty(dbAddresses)) {
                mysqlEventParser.setMasterInfo(new AuthenticationInfo(dbAddresses.get(0),
                    parameters.getDbUsername(),
                    parameters.getDbPassword(),
                    parameters.getDefaultDatabaseName()));

                if (dbAddresses.size() > 1) {
                    mysqlEventParser.setStandbyInfo(new AuthenticationInfo(dbAddresses.get(1),
                        parameters.getDbUsername(),
                        parameters.getDbPassword(),
                        parameters.getDefaultDatabaseName()));
                }
            }

            if (!CollectionUtils.isEmpty(parameters.getPositions())) {
                EntryPosition masterPosition = JsonUtils.unmarshalFromString(parameters.getPositions().get(0),
                    EntryPosition.class);
                // binlog位置参数
                mysqlEventParser.setMasterPosition(masterPosition);

                if (parameters.getPositions().size() > 1) {
                    EntryPosition standbyPosition = JsonUtils.unmarshalFromString(parameters.getPositions().get(0),
                        EntryPosition.class);
                    mysqlEventParser.setStandbyPosition(standbyPosition);
                }
            }
            mysqlEventParser.setFallbackIntervalInSeconds(parameters.getFallbackIntervalInSeconds());
            mysqlEventParser.setProfilingEnabled(false);
            eventParser = mysqlEventParser;
        } else if (type.isLocalBinlog()) {
            LocalBinlogEventParser localBinlogEventParser = new LocalBinlogEventParser();
            localBinlogEventParser.setDestination(destination);
            localBinlogEventParser.setBufferSize(parameters.getReceiveBufferSize());
            localBinlogEventParser.setConnectionCharset(Charset.forName(parameters.getConnectionCharset()));
            localBinlogEventParser.setConnectionCharsetNumber(parameters.getConnectionCharsetNumber());
            localBinlogEventParser.setDirectory(parameters.getLocalBinlogDirectory());
            localBinlogEventParser.setProfilingEnabled(false);
            localBinlogEventParser.setDetectingEnable(parameters.getDetectingEnable());
            localBinlogEventParser.setDetectingIntervalInSeconds(parameters.getDetectingIntervalInSeconds());
            // 数据库信息，反查表结构时需要
            if (!CollectionUtils.isEmpty(dbAddresses)) {
                localBinlogEventParser.setMasterInfo(new AuthenticationInfo(dbAddresses.get(0),
                    parameters.getDbUsername(),
                    parameters.getDbPassword(),
                    parameters.getDefaultDatabaseName()));

            }
            eventParser = localBinlogEventParser;
        } else if (type.isOracle()) {
            throw new CanalException("unsupport SourcingType for " + type);
        } else {
            throw new CanalException("unsupport SourcingType for " + type);
        }

        if (eventParser instanceof AbstractEventParser) { // add transaction
                                                          // support at
                                                          // 2012-12-06
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            abstractEventParser.setTransactionSize(parameters.getTransactionSize());
            abstractEventParser.setLogPositionManager(initLogPositionManager());
            abstractEventParser.setAlarmHandler(getAlarmHandler());
            abstractEventParser.setEventSink(getEventSink());

            if (StringUtils.isNotEmpty(filter)) {
                AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(filter);
                abstractEventParser.setEventFilter(aviaterFilter);
            }
        }
        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;

            // 初始化haController，绑定与eventParser的关系，haController会控制eventParser
            CanalHAController haController = initHaController();
            mysqlEventParser.setHaController(haController);
        }
        return eventParser;
    }

    protected CanalHAController initHaController() {
        logger.info("init haController begin...");
        HAMode haMode = parameters.getHaMode();
        CanalHAController haController = null;
        if (haMode.isHeartBeat()) {
            haController = new HeartBeatHAController();
            ((HeartBeatHAController) haController).setDetectingRetryTimes(parameters.getDetectingRetryTimes());
            ((HeartBeatHAController) haController).setSwitchEnable(parameters.getHeartbeatHaEnable());
        } else {
            throw new CanalException("unsupport HAMode for " + haMode);
        }
        logger.info("init haController end! \n\t load CanalHAController:{}", haController.getClass().getName());

        return haController;
    }

    protected CanalLogPositionManager initLogPositionManager() {
        logger.info("init logPositionPersistManager begin...");
        IndexMode indexMode = parameters.getIndexMode();
        CanalLogPositionManager logPositionManager = null;
        if (indexMode.isMemory()) {
            logPositionManager = new MemoryLogPositionManager();
        } else if (indexMode.isZookeeper()) {
            logPositionManager = new ZooKeeperLogPositionManager();
            ((ZooKeeperLogPositionManager) logPositionManager).setZkClientx(getZkclientx());
        } else if (indexMode.isMixed()) {
            logPositionManager = new PeriodMixedLogPositionManager();

            ZooKeeperLogPositionManager zooKeeperLogPositionManager = new ZooKeeperLogPositionManager();
            zooKeeperLogPositionManager.setZkClientx(getZkclientx());
            ((PeriodMixedLogPositionManager) logPositionManager).setZooKeeperLogPositionManager(zooKeeperLogPositionManager);
        } else if (indexMode.isMeta()) {
            logPositionManager = new MetaLogPositionManager();
            ((MetaLogPositionManager) logPositionManager).setMetaManager(metaManager);
        } else if (indexMode.isMemoryMetaFailback()) {
            MemoryLogPositionManager primaryLogPositionManager = new MemoryLogPositionManager();
            MetaLogPositionManager failbackLogPositionManager = new MetaLogPositionManager();
            failbackLogPositionManager.setMetaManager(metaManager);

            logPositionManager = new FailbackLogPositionManager();
            ((FailbackLogPositionManager) logPositionManager).setPrimary(primaryLogPositionManager);
            ((FailbackLogPositionManager) logPositionManager).setFailback(failbackLogPositionManager);
        } else {
            throw new CanalException("unsupport indexMode for " + indexMode);
        }

        logger.info("init logPositionManager end! \n\t load CanalLogPositionManager:{}", logPositionManager.getClass()
            .getName());

        return logPositionManager;
    }

    protected void startEventParserInternal(CanalEventParser eventParser) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            abstractEventParser.setAlarmHandler(getAlarmHandler());
        }

        super.startEventParserInternal(eventParser);
    }

    private int getGroupSize() {
        List<List<DataSourcing>> groupDbAddresses = parameters.getGroupDbAddresses();
        if (!CollectionUtils.isEmpty(groupDbAddresses)) {
            return groupDbAddresses.get(0).size();
        } else {
            // 可能是基于tddl的启动
            return 1;
        }
    }

    private synchronized ZkClientx getZkclientx() {
        // 做一下排序，保证相同的机器只使用同一个链接
        List<String> zkClusters = new ArrayList<String>(parameters.getZkClusters());
        Collections.sort(zkClusters);

        return ZkClientx.getZkClient(StringUtils.join(zkClusters, ";"));
    }

    // =====================================

    public String getDestination() {
        return destination;
    }

    public CanalMetaManager getMetaManager() {
        return metaManager;
    }

    public CanalEventStore<Event> getEventStore() {
        return eventStore;
    }

    public CanalEventParser getEventParser() {
        return eventParser;
    }

    public CanalEventSink<List<Entry>> getEventSink() {
        return eventSink;
    }

    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

}

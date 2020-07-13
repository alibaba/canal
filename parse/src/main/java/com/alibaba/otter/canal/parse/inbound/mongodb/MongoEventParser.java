package com.alibaba.otter.canal.parse.inbound.mongodb;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.PositionNotFoundException;
import com.alibaba.otter.canal.parse.inbound.ParserExceptionHandler;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.ChangeStreamEvent;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.ChangeStreamEventConverter;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.google.common.collect.Lists;
import com.taobao.tddl.dbsync.binlog.exception.TableIdNotFoundException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MongoEventParser
 *
 * @author jiabao.sun 2020-7-13
 * @version 1.0.0
 */
public class MongoEventParser extends AbstractCanalLifeCycle implements CanalEventParser {

    private   final Logger                           logger                     = LoggerFactory.getLogger(MongoEventParser.class);

    private   CanalLogPositionManager                logPositionManager         = null;
    private   CanalEventSink<List<CanalEntry.Entry>> eventSink                  = null;
    private   AviaterRegexFilter                     eventFilter                = null;
    private   AviaterRegexFilter                     eventBlackFilter           = null;

    // 字段过滤
    private   String                                 fieldFilter;
    private   Map<String, List<String>>              fieldFilterMap;
    private   String                                 fieldBlackFilter;
    private   Map<String, List<String>>              fieldBlackFilterMap;

    private   CanalAlarmHandler                      alarmHandler               = null;

    //指定binlog位置
    private   EntryPosition                          specifiedPosition;

    // 统计参数
    private   AtomicBoolean                          profilingEnabled           = new AtomicBoolean(false);    // profile开关参数
    private   AtomicLong                             receivedEventCount         = new AtomicLong();
    private   AtomicLong                             parsedEventCount           = new AtomicLong();
    private   AtomicLong                             consumedEventCount         = new AtomicLong();
    private   long                                   parsingInterval            = -1;
    private   long                                   processingInterval         = -1;

    private   final AtomicLong                       eventsPublishBlockingTime  = new AtomicLong(0L);

    // 认证信息
    private   volatile MongoAuthenticationInfo       authenticationInfo         = null;
    private   String                                 destination;

    // binLogParser
    private   ChangeStreamEventConverter             binlogParser               = null;

    private   Thread                                 parseThread                = null;

    private   Thread.UncaughtExceptionHandler        handler                    = new Thread.UncaughtExceptionHandler() {
                                                                                      public void uncaughtException(Thread t, Throwable e) {
                                                                                          logger.error("parse events has an error",e);
                                                                                      }
                                                                                  };

    private   EntrySinkDelegate                      entrySinkDelegate          = null;

    private   long                                   lastEntryTime              = 0L;

    private   Throwable                              exception                  = null;

    private   boolean                                parallel                   = true;                                    // 是否开启并行解析模式
    private   Integer                                parallelThreadSize         = Runtime.getRuntime().availableProcessors() * 60 / 100;     // 60%的能力跑解析,剩余部分处理网络
    private   int                                    parallelBufferSize         = 256;                                     // 必须为2的幂
    private   MongoMultiStageCoprocessor             multiStageCoprocessor;
    private   ParserExceptionHandler                 parserExceptionHandler;

    private ChangeStreamEventConverter buildParser() {
        ChangeStreamEventConverter converter = new ChangeStreamEventConverter();
        if (eventFilter != null) {
            converter.setNameFilter(eventFilter);
        }

        if (eventBlackFilter != null) {
            converter.setNameBlackFilter(eventBlackFilter);
        }
        converter.setFieldFilterMap(getFieldFilterMap());
        converter.setFieldBlackFilterMap(getFieldBlackFilterMap());
        return converter;
    }

    protected MongoConnection buildMongoConnection() {
        MongoConnection connection = new MongoConnection(authenticationInfo);
        if (eventFilter != null) {
            connection.setEventRegex(eventFilter.getPattern());
        }
        if (eventBlackFilter != null) {
            connection.setEventBlackRegex(eventBlackFilter.getPattern());
        }
        connection.setReceivedEventCount(receivedEventCount);
        return connection;
    }

    protected MongoMultiStageCoprocessor buildMultiStageCoprocessor() {
        MongoMultiStageCoprocessor mongoMultiStageCoprocessor = new MongoMultiStageCoprocessor(parallelBufferSize,
                parallelThreadSize,
                binlogParser,
                entrySinkDelegate,
                destination);
        mongoMultiStageCoprocessor.setEventsPublishBlockingTime(eventsPublishBlockingTime);
        return mongoMultiStageCoprocessor;
    }

    protected void preDump(MongoConnection connection) {
        logger.info("preDump mongo oplog to destination {}", destination);
    }

    protected void afterDump(MongoConnection connection) {
        logger.info("afterDump mongo oplog to destination {}", destination);
    }

    public MongoEventParser(){
        // 初始化一下
        entrySinkDelegate = new EntrySinkDelegate() {
            @Override
            public void sink(List<CanalEntry.Entry> entries) throws InterruptedException {
                boolean succeed = consumeTheEventAndProfilingIfNecessary(entries);
                if (!running) {
                    return;
                }

                if (!succeed) {
                    throw new CanalParseException("consume failed!");
                }

                LogPosition position = buildLastPosition(entries.get(entries.size() - 1));
                if (position != null) { // 可能position为空
                    logPositionManager.persistLogPosition(MongoEventParser.this.destination, position);
                }
            }
        };
    }

    public void start() {
        super.start();
        MDC.put("destination", destination);

        // 构造bin log parser
        binlogParser = buildParser();// 初始化一下BinLogParser
        binlogParser.start();

        // 启动工作线程
        parseThread = new Thread(new Runnable() {

            public void run() {
                MDC.put("destination", String.valueOf(destination));
                MongoConnection mongoConnection = null;
                while (running) {
                    try {
                        // 开始执行replication
                        // 1. 构造Mongo连接
                        mongoConnection = buildMongoConnection();

                        // 2. 执行dump前的准备工作
                        preDump(mongoConnection);

                        mongoConnection.connect();// 链接

                        // 3. 获取最后的位置信息
                        long start = System.currentTimeMillis();
                        logger.warn("---> begin to find start position, it will be long time for reset or first position");
                        EntryPosition position = findStartPosition(mongoConnection);
                        final EntryPosition startPosition = position;
                        if (startPosition == null) {
                            throw new PositionNotFoundException("can't find start position for " + destination);
                        }

                        long end = System.currentTimeMillis();
                        logger.warn("---> find start position successfully, {}", startPosition.toString() + " cost : "
                                + (end - start)
                                + "ms , the next step is oplog dump");


                        SinkFunction<ChangeStreamEvent> sinkHandler;
                        // 4. 开始dump数据
                        if (parallel) {
                            // build stage processor
                            multiStageCoprocessor = buildMultiStageCoprocessor();
                            multiStageCoprocessor.start();

                            sinkHandler = new SinkFunction<ChangeStreamEvent>() {
                                public boolean sink(ChangeStreamEvent changeStreamEvent) {
                                    return multiStageCoprocessor.publish(changeStreamEvent);
                                }
                            };

                        } else {
                            sinkHandler = new SinkFunction<ChangeStreamEvent>() {

                                private LogPosition lastPosition;

                                public boolean sink(ChangeStreamEvent event) {
                                    try {
                                        CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, false);

                                        if (!running) {
                                            return false;
                                        }

                                        if (entry != null) {
                                            exception = null; // 有正常数据流过，清空exception
                                            entrySinkDelegate.sink(Lists.newArrayList(entry));
                                            // 记录一下对应的positions
                                            this.lastPosition = buildLastPosition(entry);
                                            // 记录一下最后一次有数据的时间
                                            lastEntryTime = System.currentTimeMillis();
                                        }
                                        return running;
                                    } catch (TableIdNotFoundException e) {
                                        throw e;
                                    } catch (Throwable e) {
                                        if (e.getCause() instanceof TableIdNotFoundException) {
                                            throw (TableIdNotFoundException) e.getCause();
                                        }
                                        // 记录一下，出错的位点信息
                                        processSinkError(e,
                                                this.lastPosition,
                                                startPosition.getJournalName(),
                                                startPosition.getPosition());
                                        throw new CanalParseException(e); // 继续抛出异常，让上层统一感知
                                    }
                                }

                            };
                        }

                        if (startPosition.getPosition() != null) {
                            mongoConnection.dump(new BsonTimestamp(startPosition.getPosition()), sinkHandler);
                        } else {
                            mongoConnection.dump(startPosition.getTimestamp(), sinkHandler);
                        }

                    } catch (TableIdNotFoundException e) {
                        exception = e;
                        logger.error("dump address {} has an error, retrying. caused by ",
                                authenticationInfo.getHosts(), e);
                    } catch (Throwable e) {
                        processDumpError(e);
                        exception = e;
                        if (!running) {
                            if (!(e instanceof java.nio.channels.ClosedByInterruptException
                                    || e.getCause() instanceof java.nio.channels.ClosedByInterruptException)) {
                                throw new CanalParseException(String.format("dump address %s has an error, retrying. ",
                                        authenticationInfo.getHosts()), e);
                            }
                        } else {
                            logger.error("dump address {} has an error, retrying. caused by ",
                                    authenticationInfo.getHosts(), e);
                            sendAlarm(destination, ExceptionUtils.getFullStackTrace(e));
                        }
                        if (parserExceptionHandler != null) {
                            parserExceptionHandler.handle(e);
                        }
                    } finally {
                        // 重新置为中断状态
                        Thread.interrupted();
                        // 关闭一下链接
                        afterDump(mongoConnection);
                        try {
                            if (mongoConnection != null) {
                                mongoConnection.disconnect();
                            }
                        } catch (IOException e1) {
                            if (!running) {
                                throw new CanalParseException(String.format("disconnect address %s has an error, retrying. ",
                                        authenticationInfo.getHosts().toString()),
                                        e1);
                            } else {
                                logger.error("disconnect address {} has an error, retrying., caused by ",
                                        authenticationInfo.getHosts().toString(),
                                        e1);
                            }
                        }
                    }
                    // 出异常了，退出sink消费，释放一下状态
                    eventSink.interrupt();
                    binlogParser.reset();// 重新置位
                    if (multiStageCoprocessor != null && multiStageCoprocessor.isStart()) {
                        // 处理 RejectedExecutionException
                        try {
                            multiStageCoprocessor.stop();
                        } catch (Throwable t) {
                            logger.debug("multi processor rejected:", t);
                        }
                    }

                    if (running) {
                        // sleep一段时间再进行重试
                        try {
                            Thread.sleep(10000 + RandomUtils.nextInt(10000));
                        } catch (InterruptedException e) {
                        }
                    }
                }
                MDC.remove("destination");
            }
        });

        parseThread.setUncaughtExceptionHandler(handler);
        parseThread.setName(String.format("destination = %s , address = %s , EventParser",
                destination,
                authenticationInfo == null ? null : authenticationInfo.getHosts()));
        parseThread.start();
    }

    public void stop() {
        super.stop();

        parseThread.interrupt(); // 尝试中断
        eventSink.interrupt();

        if (multiStageCoprocessor != null && multiStageCoprocessor.isStart()) {
            try {
                multiStageCoprocessor.stop();
            } catch (Throwable t) {
                logger.debug("multi processor rejected:", t);
            }
        }

        try {
            parseThread.join();// 等待其结束
        } catch (InterruptedException e) {
            // ignore
        }

        if (binlogParser.isStart()) {
            binlogParser.stop();
        }
    }

    /**
     * sink oplog
     */
    protected boolean consumeTheEventAndProfilingIfNecessary(List<CanalEntry.Entry> entrys) throws CanalSinkException,
            InterruptedException {
        long startTs = -1;
        boolean enabled = getProfilingEnabled();
        if (enabled) {
            startTs = System.currentTimeMillis();
        }

        boolean result = eventSink.sink(entrys, authenticationInfo.getPrimaryINetSocketAddresses(), destination);

        if (enabled) {
            this.processingInterval = System.currentTimeMillis() - startTs;
        }

        if (consumedEventCount.incrementAndGet() < 0) {
            consumedEventCount.set(0);
        }

        return result;
    }

    /**
     * parse oplog
     */
    protected CanalEntry.Entry parseAndProfilingIfNecessary(ChangeStreamEvent bod, boolean isSeek) throws Exception {
        long startTs = -1;
        boolean enabled = getProfilingEnabled();
        if (enabled) {
            startTs = System.currentTimeMillis();
        }

        CanalEntry.Entry event = binlogParser.parse(bod, isSeek);
        if (enabled) {
            this.parsingInterval = System.currentTimeMillis() - startTs;
        }

        if (parsedEventCount.incrementAndGet() < 0) {
            parsedEventCount.set(0);
        }

        return event;
    }

    public Boolean getProfilingEnabled() {
        return profilingEnabled.get();
    }

    protected EntryPosition findStartPosition(MongoConnection connection) throws IOException {
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {// 找不到历史成功记录

            EntryPosition entryPosition = null;
            if (specifiedPosition != null &&
                    (specifiedPosition.getTimestamp() != null || specifiedPosition.getPosition() != null )) {
                // 指定位点，从指定位点开始消费
                entryPosition = specifiedPosition;
            } else {
                // 默认从当前最后一个位置进行消费
                entryPosition = new EntryPosition();

                Date lastWriteDate = connection.getPrimary().getLastWriteDate();
                long timestamp = lastWriteDate == null ? System.currentTimeMillis() : lastWriteDate.getTime();

                entryPosition.setJournalName(ChangeStreamEvent.LOG_FILE_COLLECTION);
                entryPosition.setPosition(null);
                entryPosition.setTimestamp(timestamp);
            }

            return entryPosition;
        } else {
            logger.warn("prepare to find start position just last position\n {}",
                    JsonUtils.marshalToString(logPosition));
            return logPosition.getPostion();
        }
    }

    protected LogPosition buildLastPosition(CanalEntry.Entry entry) { // 初始化一下
        LogPosition logPosition = new LogPosition();
        EntryPosition position = new EntryPosition();

        position.setJournalName(entry.getHeader().getLogfileName());
        //mongo position 记录cluster time
        position.setPosition(entry.getHeader().getLogfileOffset());
        position.setTimestamp(entry.getHeader().getExecuteTime());

        logPosition.setPostion(position);

        LogIdentity identity = new LogIdentity(authenticationInfo.getPrimaryINetSocketAddresses(), -1L);
        logPosition.setIdentity(identity);
        return logPosition;
    }

    protected void processSinkError(Throwable e, LogPosition lastPosition, String startBinlogFile, Long startPosition) {
        if (lastPosition != null) {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s]",
                    lastPosition.getPostion()),
                    e);
        } else {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s,%s]",
                    startBinlogFile,
                    startPosition), e);
        }
    }

    protected void processDumpError(Throwable e) {
        if (e instanceof IOException) {
            logger.warn("parse oplog error", e);
        }
    }

    public void sendAlarm(String destination, String msg) {
        if (this.alarmHandler != null) {
            this.alarmHandler.sendAlarm(destination, msg);
        }
    }

    /**
     * 解析字段过滤规则
     */
    private Map<String, List<String>> parseFieldFilterMap(String config) {

        Map<String, List<String>> map = new HashMap<String, List<String>>();

        if (StringUtils.isNotBlank(config)) {
            for (String filter : config.split(",")) {
                if (StringUtils.isBlank(filter)) {
                    continue;
                }

                String[] filterConfig = filter.split(":");
                if (filterConfig.length != 2) {
                    continue;
                }

                map.put(filterConfig[0].trim().toUpperCase(), Arrays.asList(filterConfig[1].trim().toUpperCase().split("/")));
            }
        }

        return map;
    }

    public void setEventFilter(AviaterRegexFilter eventFilter) {
        this.eventFilter = eventFilter;
    }

    public void setEventBlackFilter(AviaterRegexFilter eventBlackFilter) {
        this.eventBlackFilter = eventBlackFilter;
    }

    public Long getParsedEventCount() {
        return parsedEventCount.get();
    }

    public Long getConsumedEventCount() {
        return consumedEventCount.get();
    }

    public void setProfilingEnabled(boolean profilingEnabled) {
        this.profilingEnabled = new AtomicBoolean(profilingEnabled);
    }

    public long getParsingInterval() {
        return parsingInterval;
    }

    public long getProcessingInterval() {
        return processingInterval;
    }

    public void setEventSink(CanalEventSink<List<CanalEntry.Entry>> eventSink) {
        this.eventSink = eventSink;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setBinlogParser(ChangeStreamEventConverter binlogParser) {
        this.binlogParser = binlogParser;
    }

    public ChangeStreamEventConverter getBinlogParser() {
        return binlogParser;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

    public CanalAlarmHandler getAlarmHandler() {
        return this.alarmHandler;
    }

    public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
        this.logPositionManager = logPositionManager;
    }

    public CanalLogPositionManager getLogPositionManager() {
        return logPositionManager;
    }

    public Throwable getException() {
        return exception;
    }

    public boolean isParallel() {
        return parallel;
    }

    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }

    public int getParallelThreadSize() {
        return parallelThreadSize;
    }

    public void setParallelThreadSize(Integer parallelThreadSize) {
        if (parallelThreadSize != null) {
            this.parallelThreadSize = parallelThreadSize;
        }
    }

    public Integer getParallelBufferSize() {
        return parallelBufferSize;
    }

    public void setParallelBufferSize(int parallelBufferSize) {
        this.parallelBufferSize = parallelBufferSize;
    }

    public ParserExceptionHandler getParserExceptionHandler() {
        return parserExceptionHandler;
    }

    public void setParserExceptionHandler(ParserExceptionHandler parserExceptionHandler) {
        this.parserExceptionHandler = parserExceptionHandler;
    }

    public String getFieldFilter() {
        return fieldFilter;
    }

    public void setFieldFilter(String fieldFilter) {
        this.fieldFilter = fieldFilter.trim();
        this.fieldFilterMap = parseFieldFilterMap(fieldFilter);
    }

    public String getFieldBlackFilter() {
        return fieldBlackFilter;
    }

    public void setFieldBlackFilter(String fieldBlackFilter) {
        this.fieldBlackFilter = fieldBlackFilter;
        this.fieldBlackFilterMap = parseFieldFilterMap(fieldBlackFilter);
    }

    public void setSpecifiedPosition(EntryPosition specifiedPosition) {
        this.specifiedPosition = specifiedPosition;
    }

    public EntryPosition getSpecifiedPosition() {
        return specifiedPosition;
    }

    public MongoAuthenticationInfo getAuthenticationInfo() {
        return authenticationInfo;
    }

    public void setAuthenticationInfo(MongoAuthenticationInfo authenticationInfo) {
        this.authenticationInfo = authenticationInfo;
    }

    public AtomicLong getEventsPublishBlockingTime() {
        return eventsPublishBlockingTime;
    }

    public AtomicLong getReceivedEventCount() {
        return receivedEventCount;
    }

    public void setReceivedEventCount(AtomicLong receivedEventCount) {
        this.receivedEventCount = receivedEventCount;
    }

    public Map<String, List<String>> getFieldFilterMap() {
        return fieldFilterMap;
    }

    public Map<String, List<String>> getFieldBlackFilterMap() {
        return fieldBlackFilterMap;
    }

}

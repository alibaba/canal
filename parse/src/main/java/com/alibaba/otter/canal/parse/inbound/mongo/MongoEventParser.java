package com.alibaba.otter.canal.parse.inbound.mongo;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.TableIdNotFoundException;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.EventTransactionBuffer;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mongo.meta.OpLogMeta;
import com.alibaba.otter.canal.parse.inbound.mongo.support.HaAuthentication;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.index.FileMixedLogPositionManager;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * mongodb parser
 * @author dsqin
 * @date 2018/5/15
 */
public class MongoEventParser extends AbstractCanalLifeCycle implements CanalEventParser {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected CanalLogPositionManager logPositionManager = null;
    protected CanalEventSink<List<CanalEntry.Entry>> eventSink = null;
    protected CanalEventFilter eventFilter = null;
    protected CanalEventFilter eventBlackFilter = null;

    private CanalAlarmHandler alarmHandler = null;

    // 统计参数
    protected AtomicBoolean profilingEnabled = new AtomicBoolean(false);                // profile开关参数
    protected AtomicLong receivedEventCount = new AtomicLong();
    protected AtomicLong parsedEventCount = new AtomicLong();
    protected AtomicLong consumedEventCount = new AtomicLong();
    protected long parsingInterval = -1;
    protected long processingInterval = -1;

    protected volatile HaAuthentication runningInfo;
    protected String destination;

    // binLogParser
    protected BinlogParser binlogParser               = null;

    protected Thread parseThread = null;

    protected String mongoURI = null;

    protected String positionFilePath = null;

    protected MongoConnection mongoConnection = null;

    protected CanalLogPositionManager canalLogPositionManager = null;

    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        public void uncaughtException(Thread t,
                                      Throwable e) {
            logger.error("parse events has an error",
                    e);
        }
    };

    protected EventTransactionBuffer transactionBuffer;
    protected int transactionSize = 1024;
    protected AtomicBoolean needTransactionPosition = new AtomicBoolean(false);
    protected long lastEntryTime = 0L;
    protected volatile boolean detectingEnable = true;                                    // 是否开启心跳检查
    protected Integer detectingIntervalInSeconds = 3;                                       // 检测频率
    protected volatile Timer timer;
    protected TimerTask heartBeatTimerTask;
    protected Throwable exception = null;

    public MongoEventParser() {

        // 初始化一下
        transactionBuffer = new EventTransactionBuffer(new EventTransactionBuffer.TransactionFlushCallback() {

            public void flush(List<CanalEntry.Entry> transaction) throws InterruptedException {
                boolean successed = consumeTheEventAndProfilingIfNecessary(transaction);
                if (!running) {
                    return;
                }

                if (!successed) {
                    throw new CanalParseException("consume failed!");
                }

                LogPosition position = buildLastTransactionPosition(transaction);
                if (position != null) { // 可能position为空
                    logPositionManager.persistLogPosition(MongoEventParser.this.destination, position);
                }
            }
        });
    }

    protected void preDump(MongoConnection mongoConnection) {

        try {
            runningInfo = mongoConnection.buildClient();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    protected void afterDump(MongoConnection mongoConnection) {

        mongoConnection.destoryClient();
    }

    protected MongoConnection buildMongoConnection() {

        MongoConnection mongoConnection =  new MongoConnection();
        mongoConnection.setUri(mongoURI);
        return mongoConnection;
    }

    protected BinlogParser buildBinLogParser() {
        OpLogParser opLogParser = new OpLogParser();
        return opLogParser;
    }

    protected FileMixedLogPositionManager buildLogPositionManager() {

        MemoryLogPositionManager memoryLogPositionManager = new MemoryLogPositionManager();

        File positionFile = new File(positionFilePath);
//        if (!positionFile.exists()) {
//            try {
//                positionFile;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        FileMixedLogPositionManager fileMixedLogPositionManager = new FileMixedLogPositionManager(positionFile, 30000, memoryLogPositionManager);

        return fileMixedLogPositionManager;
    }

    @Override
    public void start() {

        super.start();
        MDC.put("destination", destination);
        // 配置transaction buffer
        // 初始化缓冲队列
        transactionBuffer.setBufferSize(transactionSize);// 设置buffer大小
        transactionBuffer.start();

        binlogParser = buildBinLogParser();
        binlogParser.start();

        logPositionManager = buildLogPositionManager();
        logPositionManager.start();

        parseThread = new Thread(new Runnable() {
            @Override
            public void run() {

                mongoConnection = buildMongoConnection();
                preDump(mongoConnection);

                while (running) {

                    try {

                        EntryPosition position = findStartPosition();
                        final EntryPosition startPosition = position;
                        logger.info("find start position : {}", null != position ? position.toString() : "empty");


                        final SinkFunction sinkHandler = new SinkFunction<OpLogMeta>() {

                            private LogPosition lastPosition;

                            public boolean sink(OpLogMeta event) {
                                try {
                                    CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, false);

                                    if (!running) {
                                        return false;
                                    }

                                    if (entry != null) {
                                        transactionBuffer.add(entry);
                                        // 记录一下对应的positions
                                        this.lastPosition = buildLastPosition(entry);
                                        // 记录一下最后一次有数据的时间
                                        lastEntryTime = System.currentTimeMillis();
                                        exception = null;
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
                                } finally {
                                    // 重新置为中断状态
                                    Thread.interrupted();

                                }
                            }
                        };

                        mongoConnection.seek(position, sinkHandler);

                    } catch (Throwable e) {
                        exception = e;
                        if (!running) {
                            if (!(e instanceof java.nio.channels.ClosedByInterruptException || e.getCause() instanceof java.nio.channels.ClosedByInterruptException)) {
                                throw new CanalParseException(String.format("dump address %s has an error, retrying. ",
                                        runningInfo.getAddressIdentity().toString()), e);
                            }
                        } else {
                            logger.error(String.format("dump address %s has an error, retrying. caused by ",
                                    runningInfo.getAddressIdentity().toString()), e);
                        }
                    }
//                    finally {
//                        // 重新置为中断状态
//                        Thread.interrupted();
//                    }
//                    // 出异常了，退出sink消费，释放一下状态
//                    eventSink.interrupt();
//                    transactionBuffer.reset();// 重置一下缓冲队列，重新记录数据
//
//                    if (running) {
//                        // sleep一段时间再进行重试
//                        try {
//                            Thread.sleep(10000 + RandomUtils.nextInt(10000));
//                        } catch (InterruptedException e) {
//                        }
//                    }
                }

                // 关闭一下链接
                afterDump(mongoConnection);
                MDC.remove("destination");
            }
        });

        parseThread.setUncaughtExceptionHandler(handler);
        parseThread.setName(String.format("destination = %s , address = %s , EventParser",
                destination,
                runningInfo == null ? null : runningInfo.getAddressIdentity()));
        parseThread.start();
    }


    protected void processSinkError(Throwable e, LogPosition lastPosition, String startBinlogFile, long startPosition) {
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

    protected CanalEntry.Entry parseAndProfilingIfNecessary(OpLogMeta bod, boolean isSeek) throws Exception {

        boolean enabled = getProfilingEnabled();
        long startTs = -1;

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

    protected EntryPosition findStartPosition() {

        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);

        return logPosition.getPostion();
    }


    @Override
    public void stop() {
        super.stop();

        parseThread.interrupt(); // 尝试中断
        eventSink.interrupt();
        try {
            parseThread.join();// 等待其结束
        } catch (InterruptedException e) {
            // ignore
        }

        if (binlogParser.isStart()) {
            binlogParser.stop();
        }
        if (transactionBuffer.isStart()) {
            transactionBuffer.stop();
        }
    }

    protected boolean consumeTheEventAndProfilingIfNecessary(List<CanalEntry.Entry> entrys) throws CanalSinkException,
            InterruptedException {
        long startTs = -1;
        boolean enabled = getProfilingEnabled();
        if (enabled) {
            startTs = System.currentTimeMillis();
        }

        boolean result = eventSink.sink(entrys, runningInfo.getAddressIdentity(), destination);

        if (enabled) {
            this.processingInterval = System.currentTimeMillis() - startTs;
        }

        if (consumedEventCount.incrementAndGet() < 0) {
            consumedEventCount.set(0);
        }

        return result;
    }

    public Boolean getProfilingEnabled() {
        return profilingEnabled.get();
    }

    protected LogPosition buildLastTransactionPosition(List<CanalEntry.Entry> entries) { // 初始化一下
        if (null == entries || entries.isEmpty()) {
            return null;
        }
        return buildLastPosition(entries.get(0));
    }

    protected LogPosition buildLastPosition(CanalEntry.Entry entry) { // 初始化一下
        LogPosition logPosition = new LogPosition();
        EntryPosition position = new EntryPosition();
        position.setJournalName(entry.getHeader().getLogfileName());
        position.setPosition(entry.getHeader().getLogfileOffset());
        position.setTimestamp(entry.getHeader().getExecuteTime());
        // add serverId at 2016-06-28
        position.setServerId(entry.getHeader().getServerId());
        logPosition.setPostion(position);
        LogIdentity identity = new LogIdentity(runningInfo.getAddressIdentity(), -1L);
        logPosition.setIdentity(identity);
        return logPosition;
    }

    public CanalLogPositionManager getLogPositionManager() {
        return logPositionManager;
    }

    public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
        this.logPositionManager = logPositionManager;
    }

    public CanalEventSink<List<CanalEntry.Entry>> getEventSink() {
        return eventSink;
    }

    public void setEventSink(CanalEventSink<List<CanalEntry.Entry>> eventSink) {
        this.eventSink = eventSink;
    }

    public CanalEventFilter getEventFilter() {
        return eventFilter;
    }

    public void setEventFilter(CanalEventFilter eventFilter) {
        this.eventFilter = eventFilter;
    }

    public CanalEventFilter getEventBlackFilter() {
        return eventBlackFilter;
    }

    public void setEventBlackFilter(CanalEventFilter eventBlackFilter) {
        this.eventBlackFilter = eventBlackFilter;
    }

    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

    public void setProfilingEnabled(AtomicBoolean profilingEnabled) {
        this.profilingEnabled = profilingEnabled;
    }

    public AtomicLong getReceivedEventCount() {
        return receivedEventCount;
    }

    public void setReceivedEventCount(AtomicLong receivedEventCount) {
        this.receivedEventCount = receivedEventCount;
    }

    public AtomicLong getParsedEventCount() {
        return parsedEventCount;
    }

    public void setParsedEventCount(AtomicLong parsedEventCount) {
        this.parsedEventCount = parsedEventCount;
    }

    public AtomicLong getConsumedEventCount() {
        return consumedEventCount;
    }

    public void setConsumedEventCount(AtomicLong consumedEventCount) {
        this.consumedEventCount = consumedEventCount;
    }

    public long getParsingInterval() {
        return parsingInterval;
    }

    public void setParsingInterval(long parsingInterval) {
        this.parsingInterval = parsingInterval;
    }

    public long getProcessingInterval() {
        return processingInterval;
    }

    public void setProcessingInterval(long processingInterval) {
        this.processingInterval = processingInterval;
    }

    public HaAuthentication getRunningInfo() {
        return runningInfo;
    }

    public void setRunningInfo(HaAuthentication runningInfo) {
        this.runningInfo = runningInfo;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public Thread getParseThread() {
        return parseThread;
    }

    public void setParseThread(Thread parseThread) {
        this.parseThread = parseThread;
    }

    public Thread.UncaughtExceptionHandler getHandler() {
        return handler;
    }

    public void setHandler(Thread.UncaughtExceptionHandler handler) {
        this.handler = handler;
    }

    public EventTransactionBuffer getTransactionBuffer() {
        return transactionBuffer;
    }

    public void setTransactionBuffer(EventTransactionBuffer transactionBuffer) {
        this.transactionBuffer = transactionBuffer;
    }

    public int getTransactionSize() {
        return transactionSize;
    }

    public void setTransactionSize(int transactionSize) {
        this.transactionSize = transactionSize;
    }

    public AtomicBoolean getNeedTransactionPosition() {
        return needTransactionPosition;
    }

    public void setNeedTransactionPosition(AtomicBoolean needTransactionPosition) {
        this.needTransactionPosition = needTransactionPosition;
    }

    public long getLastEntryTime() {
        return lastEntryTime;
    }

    public void setLastEntryTime(long lastEntryTime) {
        this.lastEntryTime = lastEntryTime;
    }

    public boolean isDetectingEnable() {
        return detectingEnable;
    }

    public void setDetectingEnable(boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

    public Integer getDetectingIntervalInSeconds() {
        return detectingIntervalInSeconds;
    }

    public void setDetectingIntervalInSeconds(Integer detectingIntervalInSeconds) {
        this.detectingIntervalInSeconds = detectingIntervalInSeconds;
    }

    public Timer getTimer() {
        return timer;
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }

    public TimerTask getHeartBeatTimerTask() {
        return heartBeatTimerTask;
    }

    public void setHeartBeatTimerTask(TimerTask heartBeatTimerTask) {
        this.heartBeatTimerTask = heartBeatTimerTask;
    }

    public String getMongoURI() {
        return mongoURI;
    }

    public void setMongoURI(String mongoURI) {
        this.mongoURI = mongoURI;
    }

    public String getPositionFilePath() {
        return positionFilePath;
    }

    public void setPositionFilePath(String positionFilePath) {
        this.positionFilePath = positionFilePath;
    }
}

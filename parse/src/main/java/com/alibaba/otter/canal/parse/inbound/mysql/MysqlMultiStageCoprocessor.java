package com.alibaba.otter.canal.parse.inbound.mysql;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.EventTransactionBuffer;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;

/**
 * 针对解析器提供一个多阶段协同的处理
 * 
 * <pre>
 * 1. 网络接收 (单线程)
 * 2. 事件基本解析 (单线程，事件类型、DDL解析构造TableMeta、维护位点信息)
 * 3. 事件深度解析 (多线程, DML事件数据的完整解析)
 * 4. 投递到store (单线程)
 * </pre>
 * 
 * @author agapple 2018年7月3日 下午4:54:17
 * @since 1.0.26
 */
public class MysqlMultiStageCoprocessor extends AbstractCanalLifeCycle implements MultiStageCoprocessor {

    private static final int                  maxFullTimes = 10;
    private LogEventConvert                   logEventConvert;
    private EventTransactionBuffer            transactionBuffer;
    private ErosaConnection                   connection;

    private int                               parserThreadCount;
    private int                               ringBufferSize;
    private RingBuffer<MessageEvent>          disruptorMsgBuffer;
    private ExecutorService                   parserExecutor;
    private ExecutorService                   stageExecutor;
    private String                            destination;
    private volatile CanalParseException      exception;
    private AtomicLong                        eventsPublishBlockingTime;
    private GTIDSet                           gtidSet;
    private WorkerPool<MessageEvent>          workerPool;
    private BatchEventProcessor<MessageEvent> simpleParserStage;
    private BatchEventProcessor<MessageEvent> sinkStoreStage;
    private LogContext                        logContext;

    public MysqlMultiStageCoprocessor(int ringBufferSize, int parserThreadCount, LogEventConvert logEventConvert,
                                      EventTransactionBuffer transactionBuffer, String destination){
        this.ringBufferSize = ringBufferSize;
        this.parserThreadCount = parserThreadCount;
        this.logEventConvert = logEventConvert;
        this.transactionBuffer = transactionBuffer;
        this.destination = destination;
    }

    @Override
    public void start() {
        super.start();
        this.exception = null;
        this.disruptorMsgBuffer = RingBuffer.createSingleProducer(new MessageEventFactory(),
            ringBufferSize,
            new BlockingWaitStrategy());
        int tc = parserThreadCount > 0 ? parserThreadCount : 1;
        this.parserExecutor = Executors.newFixedThreadPool(tc, new NamedThreadFactory("MultiStageCoprocessor-Parser-"
                                                                                      + destination));

        this.stageExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("MultiStageCoprocessor-other-"
                                                                                    + destination));
        SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
        ExceptionHandler exceptionHandler = new SimpleFatalExceptionHandler();
        // stage 2
        this.logContext = new LogContext();
        simpleParserStage = new BatchEventProcessor<MessageEvent>(disruptorMsgBuffer,
            sequenceBarrier,
            new SimpleParserStage(logContext));
        simpleParserStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(simpleParserStage.getSequence());

        // stage 3
        SequenceBarrier dmlParserSequenceBarrier = disruptorMsgBuffer.newBarrier(simpleParserStage.getSequence());
        WorkHandler<MessageEvent>[] workHandlers = new DmlParserStage[tc];
        for (int i = 0; i < tc; i++) {
            workHandlers[i] = new DmlParserStage();
        }
        workerPool = new WorkerPool<MessageEvent>(disruptorMsgBuffer,
            dmlParserSequenceBarrier,
            exceptionHandler,
            workHandlers);
        Sequence[] sequence = workerPool.getWorkerSequences();
        disruptorMsgBuffer.addGatingSequences(sequence);

        // stage 4
        SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(sequence);
        sinkStoreStage = new BatchEventProcessor<MessageEvent>(disruptorMsgBuffer,
            sinkSequenceBarrier,
            new SinkStoreStage());
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());

        // start work
        stageExecutor.submit(simpleParserStage);
        stageExecutor.submit(sinkStoreStage);
        workerPool.start(parserExecutor);
    }

    public void setBinlogChecksum(int binlogChecksum) {
        if (binlogChecksum != LogEvent.BINLOG_CHECKSUM_ALG_OFF) {
            logContext.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
        }
    }

    @Override
    public void stop() {
        // fix bug #968，对于pool与
        workerPool.halt();
        simpleParserStage.halt();
        sinkStoreStage.halt();
        try {
            parserExecutor.shutdownNow();
            while (!parserExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                if (parserExecutor.isShutdown() || parserExecutor.isTerminated()) {
                    break;
                }

                parserExecutor.shutdownNow();
            }
        } catch (Throwable e) {
            // ignore
        }

        try {
            stageExecutor.shutdownNow();
            while (!stageExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                if (stageExecutor.isShutdown() || stageExecutor.isTerminated()) {
                    break;
                }

                stageExecutor.shutdownNow();
            }
        } catch (Throwable e) {
            // ignore
        }
        super.stop();
    }

    public boolean publish(LogBuffer buffer) {
        return this.publish(buffer, null);
    }

    /**
     * 网络数据投递
     */
    public boolean publish(LogEvent event) {
        return this.publish(null, event);
    }

    private boolean publish(LogBuffer buffer, LogEvent event) {
        if (!isStart()) {
            if (exception != null) {
                throw exception;
            }
            return false;
        }

        boolean interupted = false;
        long blockingStart = 0L;
        int fullTimes = 0;
        do {
            /**
             * 由于改为processor仅终止自身stage而不是stop，那么需要由incident标识coprocessor是否正常工作。
             * 让dump线程能够及时感知
             */
            if (exception != null) {
                throw exception;
            }
            try {
                long next = disruptorMsgBuffer.tryNext();
                MessageEvent data = disruptorMsgBuffer.get(next);
                if (buffer != null) {
                    data.setBuffer(buffer);
                } else {
                    data.setEvent(event);
                }
                disruptorMsgBuffer.publish(next);
                if (fullTimes > 0) {
                    eventsPublishBlockingTime.addAndGet(System.nanoTime() - blockingStart);
                }
                break;
            } catch (InsufficientCapacityException e) {
                if (fullTimes == 0) {
                    blockingStart = System.nanoTime();
                }
                // park
                // LockSupport.parkNanos(1L);
                applyWait(++fullTimes);
                interupted = Thread.interrupted();
                if (fullTimes % 1000 == 0) {
                    long nextStart = System.nanoTime();
                    eventsPublishBlockingTime.addAndGet(nextStart - blockingStart);
                    blockingStart = nextStart;
                }
            }
        } while (!interupted && isStart());
        return isStart();
    }

    // 处理无数据的情况，避免空循环挂死
    private void applyWait(int fullTimes) {
        int newFullTimes = fullTimes > maxFullTimes ? maxFullTimes : fullTimes;
        if (fullTimes <= 3) { // 3次以内
            Thread.yield();
        } else { // 超过3次，最多只sleep 1ms
            LockSupport.parkNanos(100 * 1000L * newFullTimes);
        }

    }

    private class SimpleParserStage implements EventHandler<MessageEvent>, LifecycleAware {

        private LogDecoder decoder;
        private LogContext context;

        public SimpleParserStage(LogContext context){
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            this.context = context;
            if (gtidSet != null) {
                context.setGtidSet(gtidSet);
            }
        }

        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                LogEvent logEvent = event.getEvent();
                if (logEvent == null) {
                    LogBuffer buffer = event.getBuffer();
                    logEvent = decoder.decode(buffer, context);
                    event.setEvent(logEvent);
                }

                int eventType = logEvent.getHeader().getType();
                TableMeta tableMeta = null;
                boolean needDmlParse = false;
                switch (eventType) {
                    case LogEvent.WRITE_ROWS_EVENT_V1:
                    case LogEvent.WRITE_ROWS_EVENT:
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((WriteRowsLogEvent) logEvent);
                        needDmlParse = true;
                        break;
                    case LogEvent.UPDATE_ROWS_EVENT_V1:
                    case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
                    case LogEvent.UPDATE_ROWS_EVENT:
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((UpdateRowsLogEvent) logEvent);
                        needDmlParse = true;
                        break;
                    case LogEvent.DELETE_ROWS_EVENT_V1:
                    case LogEvent.DELETE_ROWS_EVENT:
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((DeleteRowsLogEvent) logEvent);
                        needDmlParse = true;
                        break;
                    case LogEvent.ROWS_QUERY_LOG_EVENT:
                        needDmlParse = true;
                        break;
                    default:
                        CanalEntry.Entry entry = logEventConvert.parse(event.getEvent(), false);
                        event.setEntry(entry);
                }

                // 记录一下DML的表结构
                event.setNeedDmlParse(needDmlParse);
                event.setTable(tableMeta);
            } catch (Throwable e) {
                exception = new CanalParseException(e);
                throw exception;
            }
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    private class DmlParserStage implements WorkHandler<MessageEvent>, LifecycleAware {

        @Override
        public void onEvent(MessageEvent event) throws Exception {
            try {
                if (event.isNeedDmlParse()) {
                    int eventType = event.getEvent().getHeader().getType();
                    CanalEntry.Entry entry = null;
                    switch (eventType) {
                        case LogEvent.ROWS_QUERY_LOG_EVENT:
                            entry = logEventConvert.parse(event.getEvent(), false);
                            break;
                        default:
                            // 单独解析dml事件
                            entry = logEventConvert.parseRowsEvent((RowsLogEvent) event.getEvent(), event.getTable());
                    }

                    event.setEntry(entry);
                }
            } catch (Throwable e) {
                exception = new CanalParseException(e);
                throw exception;
            }
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    private class SinkStoreStage implements EventHandler<MessageEvent>, LifecycleAware {

        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                if (event.getEntry() != null) {
                    transactionBuffer.add(event.getEntry());
                }

                LogEvent logEvent = event.getEvent();
                if (connection instanceof MysqlConnection && logEvent.getSemival() == 1) {
                    // semi ack回报
                    ((MysqlConnection) connection).sendSemiAck(logEvent.getHeader().getLogFileName(),
                        logEvent.getHeader().getLogPos());
                }

                // clear for gc
                event.setBuffer(null);
                event.setEvent(null);
                event.setTable(null);
                event.setEntry(null);
                event.setNeedDmlParse(false);
            } catch (Throwable e) {
                exception = new CanalParseException(e);
                throw exception;
            }
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    class MessageEvent {

        private LogBuffer        buffer;
        private CanalEntry.Entry entry;
        private boolean          needDmlParse = false;
        private TableMeta        table;
        private LogEvent         event;

        public LogBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(LogBuffer buffer) {
            this.buffer = buffer;
        }

        public LogEvent getEvent() {
            return event;
        }

        public void setEvent(LogEvent event) {
            this.event = event;
        }

        public CanalEntry.Entry getEntry() {
            return entry;
        }

        public void setEntry(CanalEntry.Entry entry) {
            this.entry = entry;
        }

        public boolean isNeedDmlParse() {
            return needDmlParse;
        }

        public void setNeedDmlParse(boolean needDmlParse) {
            this.needDmlParse = needDmlParse;
        }

        public TableMeta getTable() {
            return table;
        }

        public void setTable(TableMeta table) {
            this.table = table;
        }

    }

    class SimpleFatalExceptionHandler implements ExceptionHandler {

        @Override
        public void handleEventException(final Throwable ex, final long sequence, final Object event) {
            //异常上抛，否则processEvents的逻辑会默认会mark为成功执行，有丢数据风险
            throw new CanalParseException(ex);
        }

        @Override
        public void handleOnStartException(final Throwable ex) {
        }

        @Override
        public void handleOnShutdownException(final Throwable ex) {
        }
    }

    class MessageEventFactory implements EventFactory<MessageEvent> {

        public MessageEvent newInstance() {
            return new MessageEvent();
        }
    }

    public void setLogEventConvert(LogEventConvert logEventConvert) {
        this.logEventConvert = logEventConvert;
    }

    public void setTransactionBuffer(EventTransactionBuffer transactionBuffer) {
        this.transactionBuffer = transactionBuffer;
    }

    public void setConnection(ErosaConnection connection) {
        this.connection = connection;
    }

    public void setEventsPublishBlockingTime(AtomicLong eventsPublishBlockingTime) {
        this.eventsPublishBlockingTime = eventsPublishBlockingTime;
    }

    public void setGtidSet(GTIDSet gtidSet) {
        this.gtidSet = gtidSet;
    }

}

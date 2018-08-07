package com.alibaba.otter.canal.parse.inbound.mysql;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
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
import com.taobao.tddl.dbsync.binlog.LogPosition;
import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
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

    private LogEventConvert              logEventConvert;
    private EventTransactionBuffer       transactionBuffer;
    private ErosaConnection              connection;

    private int                          parserThreadCount;
    private int                          ringBufferSize;
    private RingBuffer<MessageEvent>     disruptorMsgBuffer;
    private ExecutorService              parserExecutor;
    private ExecutorService              stageExecutor;
    private String                       destination;
    private volatile CanalParseException exception;

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

        this.parserExecutor = Executors.newFixedThreadPool(parserThreadCount,
            new NamedThreadFactory("MultiStageCoprocessor-Parser-" + destination));

        this.stageExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("MultiStageCoprocessor-other-"
                                                                                    + destination));
        SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
        ExceptionHandler exceptionHandler = new SimpleFatalExceptionHandler();
        // stage 2
        BatchEventProcessor<MessageEvent> simpleParserStage = new BatchEventProcessor<MessageEvent>(disruptorMsgBuffer,
            sequenceBarrier,
            new SimpleParserStage());
        simpleParserStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(simpleParserStage.getSequence());

        // stage 3
        SequenceBarrier dmlParserSequenceBarrier = disruptorMsgBuffer.newBarrier(simpleParserStage.getSequence());
        WorkHandler<MessageEvent>[] workHandlers = new DmlParserStage[parserThreadCount];
        for (int i = 0; i < parserThreadCount; i++) {
            workHandlers[i] = new DmlParserStage();
        }
        WorkerPool<MessageEvent> workerPool = new WorkerPool<MessageEvent>(disruptorMsgBuffer,
            dmlParserSequenceBarrier,
            exceptionHandler,
            workHandlers);
        Sequence[] sequence = workerPool.getWorkerSequences();
        disruptorMsgBuffer.addGatingSequences(sequence);

        // stage 4
        SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(sequence);
        BatchEventProcessor<MessageEvent> sinkStoreStage = new BatchEventProcessor<MessageEvent>(disruptorMsgBuffer,
            sinkSequenceBarrier,
            new SinkStoreStage());
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());

        // start work
        stageExecutor.submit(simpleParserStage);
        stageExecutor.submit(sinkStoreStage);
        workerPool.start(parserExecutor);
    }

    @Override
    public void stop() {
        try {
            parserExecutor.shutdownNow();
        } catch (Throwable e) {
            // ignore
        }

        try {
            stageExecutor.shutdownNow();
        } catch (Throwable e) {
            // ignore
        }
        super.stop();
    }

    /**
     * 网络数据投递
     */
    public boolean publish(LogBuffer buffer) {
        return publish(buffer, null);
    }

    public boolean publish(LogBuffer buffer, String binlogFileName) {
        if (!isStart()) {
            if (exception != null) {
                throw exception;
            }
            return false;
        }

        /**
         * 由于改为processor仅终止自身stage而不是stop，那么需要由incident标识coprocessor是否正常工作。
         * 让dump线程能够及时感知
         */
        if (exception != null) {
            throw exception;
        }
        boolean interupted = false;
        do {
            try {
                long next = disruptorMsgBuffer.tryNext();
                MessageEvent event = disruptorMsgBuffer.get(next);
                event.setBuffer(buffer);
                if (binlogFileName != null) {
                    event.setBinlogFileName(binlogFileName);
                }
                disruptorMsgBuffer.publish(next);
                break;
            } catch (InsufficientCapacityException e) {
                // park
                LockSupport.parkNanos(1L);
                interupted = Thread.interrupted();
            }
        } while (!interupted && isStart());
        return isStart();
    }

    @Override
    public void reset() {
        if (isStart()) {
            stop();
        }

        start();
    }

    private class SimpleParserStage implements EventHandler<MessageEvent>, LifecycleAware {

        private LogDecoder decoder;
        private LogContext context;

        public SimpleParserStage(){
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            context = new LogContext();
        }

        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                LogBuffer buffer = event.getBuffer();
                if (StringUtils.isNotEmpty(event.getBinlogFileName())
                    && !context.getLogPosition().getFileName().equals(event.getBinlogFileName())) {
                    // set roate binlog file name
                    context.setLogPosition(new LogPosition(event.getBinlogFileName(), context.getLogPosition()
                        .getPosition()));
                }

                LogEvent logEvent = decoder.decode(buffer, context);
                event.setEvent(logEvent);

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
                event.setBinlogFileName(null);
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

        private String           binlogFileName;      // for local binlog parse
        private LogBuffer        buffer;
        private CanalEntry.Entry entry;
        private boolean          needDmlParse = false;
        private TableMeta        table;
        private LogEvent         event;

        public String getBinlogFileName() {
            return binlogFileName;
        }

        public void setBinlogFileName(String binlogFileName) {
            this.binlogFileName = binlogFileName;
        }

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

}

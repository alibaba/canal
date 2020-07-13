package com.alibaba.otter.canal.parse.inbound.mongodb;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.ChangeStreamEvent;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.ChangeStreamEventConverter;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.collect.Lists;
import com.lmax.disruptor.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


/**
 * 针对解析器提供一个多阶段协同的处理
 *
 * <pre>
 * 1. 网络接收 publish(单线程)
 * 2. 事件深度解析 (多线程, DML事件数据的完整解析)
 * 3. 投递到store (单线程)
 * </pre>
 */
public class MongoMultiStageCoprocessor extends AbstractCanalLifeCycle {

    private static final int                  maxFullTimes = 10;
    private ChangeStreamEventConverter logEventConvert;
    private EntrySinkDelegate                 entrySinkDelegate;

    private int                               parserThreadCount;
    private int                               ringBufferSize;
    private RingBuffer<MessageEvent>          disruptorMsgBuffer;
    private ExecutorService                   parserExecutor;
    private ExecutorService                   stageExecutor;
    private String                            destination;
    private volatile CanalParseException exception;
    private AtomicLong                        eventsPublishBlockingTime;
    private WorkerPool<MessageEvent>          workerPool;
    private BatchEventProcessor<MessageEvent> sinkStoreStage;

    public MongoMultiStageCoprocessor(int ringBufferSize, int parserThreadCount, ChangeStreamEventConverter logEventConvert,
                                      EntrySinkDelegate entrySinkDelegate, String destination){
        this.ringBufferSize = ringBufferSize;
        this.parserThreadCount = parserThreadCount;
        this.logEventConvert = logEventConvert;
        this.entrySinkDelegate = entrySinkDelegate;
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

        ExceptionHandler exceptionHandler = new SimpleFatalExceptionHandler();

        // stage 2
        SequenceBarrier dmlParserSequenceBarrier = disruptorMsgBuffer.newBarrier();
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

        // stage 3
        SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(sequence);
        sinkStoreStage = new BatchEventProcessor<MessageEvent>(disruptorMsgBuffer,
                sinkSequenceBarrier,
                new SinkStoreStage());
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());

        // start work
        stageExecutor.submit(sinkStoreStage);
        workerPool.start(parserExecutor);
    }

    @Override
    public void stop() {
        // fix bug #968，对于pool与
        workerPool.halt();
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

    public boolean publish(ChangeStreamEvent event) {
        if (!isStart()) {
            if (exception != null) {
                throw exception;
            }
            return false;
        }

        boolean interrupted = false;
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
                data.setEvent(event);
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
                interrupted = Thread.interrupted();
                if (fullTimes % 1000 == 0) {
                    long nextStart = System.nanoTime();
                    eventsPublishBlockingTime.addAndGet(nextStart - blockingStart);
                    blockingStart = nextStart;
                }
            }
        } while (!interrupted && isStart());
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

    private class DmlParserStage implements WorkHandler<MessageEvent>, LifecycleAware {

        @Override
        public void onEvent(MessageEvent event) throws Exception {
            try {
                ChangeStreamEvent logEvent = event.getEvent();
                if (logEvent == null) {
                    return;
                }

                CanalEntry.Entry entry = logEventConvert.parse(event.getEvent(), false);
                event.setEntry(entry);
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
                    entrySinkDelegate.sink(Lists.newArrayList(event.getEntry()));
                }
                // clear for gc
                event.setEvent(null);
                event.setEntry(null);
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
        private CanalEntry.Entry        entry;
        private ChangeStreamEvent event;

        public ChangeStreamEvent getEvent() {
            return event;
        }

        public void setEvent(ChangeStreamEvent event) {
            this.event = event;
        }

        public CanalEntry.Entry getEntry() {
            return entry;
        }

        public void setEntry(CanalEntry.Entry entry) {
            this.entry = entry;
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

    public void setLogEventConvert(ChangeStreamEventConverter logEventConvert) {
        this.logEventConvert = logEventConvert;
    }

    public void setEventsPublishBlockingTime(AtomicLong eventsPublishBlockingTime) {
        this.eventsPublishBlockingTime = eventsPublishBlockingTime;
    }

}

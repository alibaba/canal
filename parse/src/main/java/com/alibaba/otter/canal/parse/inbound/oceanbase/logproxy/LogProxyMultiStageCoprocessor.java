package com.alibaba.otter.canal.parse.inbound.oceanbase.logproxy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractMultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.EventTransactionBuffer;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;
import com.oceanbase.oms.logmessage.LogMessage;

/**
 * 基于LogProxy的MultiStageCoprocessor实现
 *
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public class LogProxyMultiStageCoprocessor extends AbstractMultiStageCoprocessor<LogProxyMultiStageCoprocessor.MessageEvent> {

    private final LogProxyMessageParser logParser;
    private final int                   parserThreadCount;

    public LogProxyMultiStageCoprocessor(LogProxyMessageParser logParser, int parserThreadCount,
                                         EventTransactionBuffer transactionBuffer, int ringBufferSize,
                                         String destination) {
        super(destination, transactionBuffer, ringBufferSize);
        this.logParser = logParser;
        this.parserThreadCount = parserThreadCount;
    }

    @Override
    public void start() {
        super.start();
        disruptorMsgBuffer = RingBuffer.createSingleProducer(new MessageEventFactory(),
            ringBufferSize,
            new BlockingWaitStrategy());
        int tc = parserThreadCount > 0 ? parserThreadCount : 1;
        ExecutorService parserExecutor = Executors.newFixedThreadPool(tc,
            new NamedThreadFactory("MultiStageCoprocessor-Parser-" + destination));
        executorServices.add(parserExecutor);
        ExecutorService stageExecutor = Executors.newFixedThreadPool(1,
            new NamedThreadFactory("MultiStageCoprocessor-other-" + destination));
        executorServices.add(stageExecutor);

        SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
        ExceptionHandler<MessageEvent> exceptionHandler = new SimpleFatalExceptionHandler();

        // parse
        WorkHandler<MessageEvent>[] workHandlers = new ParserStage[tc];
        for (int i = 0; i < tc; i++) {
            workHandlers[i] = new ParserStage();
        }
        WorkerPool<MessageEvent> workerPool = new WorkerPool<>(disruptorMsgBuffer,
            sequenceBarrier,
            exceptionHandler,
            workHandlers);
        workerPools.add(workerPool);
        Sequence[] sequence = workerPool.getWorkerSequences();
        disruptorMsgBuffer.addGatingSequences(sequence);

        // sink
        SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(sequence);
        BatchEventProcessor<MessageEvent> sinkStoreStage = new BatchEventProcessor<>(disruptorMsgBuffer,
            sinkSequenceBarrier,
            new SinkStoreStage());
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        eventProcessors.add(sinkStoreStage);
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());

        // start work
        stageExecutor.submit(sinkStoreStage);
        workerPool.start(parserExecutor);
    }

    @Override
    protected void setEventData(MessageEvent messageEvent, Object data) {
        messageEvent.setLogMessage((LogMessage) data);
    }

    private class ParserStage implements WorkHandler<MessageEvent>, LifecycleAware {

        @Override
        public void onEvent(MessageEvent messageEvent) throws Exception {
            try {
                LogMessage message = messageEvent.getLogMessage();
                CanalEntry.Entry entry = logParser.parse(message, false);
                messageEvent.setEntry(entry);
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

        @Override
        public void onEvent(MessageEvent messageEvent, long sequence, boolean endOfBatch) throws Exception {
            try {
                CanalEntry.Entry entry = messageEvent.getEntry();
                if (entry != null) {
                    transactionBuffer.add(messageEvent.getEntry());
                }

                messageEvent.setLogMessage(null);
                messageEvent.setEntry(null);
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

    static class MessageEvent {

        private LogMessage       logMessage;
        private CanalEntry.Entry entry;

        public LogMessage getLogMessage() {
            return logMessage;
        }

        public void setLogMessage(LogMessage logMessage) {
            this.logMessage = logMessage;
        }

        public CanalEntry.Entry getEntry() {
            return entry;
        }

        public void setEntry(CanalEntry.Entry entry) {
            this.entry = entry;
        }
    }

    static class MessageEventFactory implements EventFactory<MessageEvent> {

        @Override
        public MessageEvent newInstance() {
            return new MessageEvent();
        }
    }
}

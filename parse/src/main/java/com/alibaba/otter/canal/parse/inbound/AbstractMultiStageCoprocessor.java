package com.alibaba.otter.canal.parse.inbound;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;

/**
 * 多阶段并发处理器接口的抽象实现类
 *
 * @param <Event> RingBuffer中存储的数据类型
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public abstract class AbstractMultiStageCoprocessor<Event> extends AbstractCanalLifeCycle implements MultiStageCoprocessor {

    protected static final int                              MAX_FULL_TIMES   = 10;
    protected              EventTransactionBuffer           transactionBuffer;
    protected              int                              ringBufferSize;
    protected              String                           destination;
    protected volatile     CanalParseException              exception        = null;
    protected              AtomicLong                       eventsPublishBlockingTime;
    protected              RingBuffer<Event>                disruptorMsgBuffer;
    protected              List<WorkerPool<Event>>          workerPools      = new ArrayList<>();
    protected              List<BatchEventProcessor<Event>> eventProcessors  = new ArrayList<>();
    protected              List<ExecutorService>            executorServices = new ArrayList<>();

    public AbstractMultiStageCoprocessor(String destination, EventTransactionBuffer transactionBuffer,
                                         int ringBufferSize) {
        this.destination = destination;
        this.transactionBuffer = transactionBuffer;
        this.ringBufferSize = ringBufferSize;
    }

    /**
     * 将data填入event结构
     *
     * @param event RingBuffer中存储的数据结构
     * @param data  要填入的数据
     */
    protected abstract void setEventData(Event event, Object data);

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        // fix bug #968
        for (WorkerPool<Event> workerPool : workerPools) {
            workerPool.halt();
        }

        for (EventProcessor processor : eventProcessors) {
            processor.halt();
        }

        for (ExecutorService executorService : executorServices) {
            try {
                executorService.shutdownNow();
                while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    if (executorService.isShutdown() || executorService.isTerminated()) {
                        break;
                    }

                    executorService.shutdownNow();
                }
            } catch (Throwable e) {
                // ignore
            }

        }
        super.stop();
    }

    @Override
    public boolean publish(Object data) {
        if (!isStart()) {
            if (exception != null) {
                throw exception;
            }
            return false;
        }

        boolean interupted;
        long blockingStart = 0L;
        int fullTimes = 0;
        do {
            /*
              由于改为processor仅终止自身stage而不是stop，那么需要由incident标识coprocessor是否正常工作。
              让dump线程能够及时感知
             */
            if (exception != null) {
                throw exception;
            }
            try {
                long next = disruptorMsgBuffer.tryNext();
                Event event = disruptorMsgBuffer.get(next);
                setEventData(event, data);
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

    /**
     * 处理无数据的情况，避免空循环挂死
     *
     * @param fullTimes 缓冲区填满的次数
     */
    private void applyWait(int fullTimes) {
        int newFullTimes = Math.min(fullTimes, MAX_FULL_TIMES);
        if (fullTimes <= 3) {
            Thread.yield();
        } else {
            LockSupport.parkNanos(100 * 1000L * newFullTimes);
        }

    }

    public class SimpleFatalExceptionHandler implements ExceptionHandler<Event> {

        @Override
        public void handleEventException(final Throwable ex, final long sequence, final Object event) {
            // 异常上抛，否则processEvents的逻辑会默认会mark为成功执行，有丢数据风险
            throw new CanalParseException(ex);
        }

        @Override
        public void handleOnStartException(final Throwable ex) {
        }

        @Override
        public void handleOnShutdownException(final Throwable ex) {
        }
    }

    public void setEventsPublishBlockingTime(AtomicLong eventsPublishBlockingTime) {
        this.eventsPublishBlockingTime = eventsPublishBlockingTime;
    }
}

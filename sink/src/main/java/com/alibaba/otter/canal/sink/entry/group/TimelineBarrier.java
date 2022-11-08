package com.alibaba.otter.canal.sink.entry.group;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.otter.canal.store.model.Event;

/**
 * 时间归并控制
 * 
 * <pre>
 * 大致设计：
 *  1. 多个队列都提交一个timestamp，判断出最小的一个timestamp做为通过的条件，然后唤醒<=该最小时间的线程通过
 *  2. 只有当多个队列都提交了一个timestamp，缺少任何一个提交，都会阻塞其他队列通过。(解决当一个库启动过慢或者发生主备切换时出现延迟等问题)
 * 
 * 存在一个假定，认为提交的timestamp是一个顺序递增，但是在两种case下会出现时间回退
 * a. 大事务时，事务头的时间会晚于事务当中数据的时间，相当于出现一个时间回退
 * b. 出现主备切换，从备机上发过来的数据会回退几秒钟
 * 
 * </pre>
 * 
 * @author jianghang 2012-10-15 下午10:01:53
 * @version 1.0.0
 */
public class TimelineBarrier implements GroupBarrier<Event> {

    protected int                 groupSize;
    protected ReentrantLock       lock           = new ReentrantLock();
    protected Condition           condition      = lock.newCondition();
    protected volatile long       threshold;
    protected BlockingQueue<Long> lastTimestamps = new PriorityBlockingQueue<>(); // 当前通道最后一次single的时间戳

    public TimelineBarrier(int groupSize){
        this.groupSize = groupSize;
        threshold = Long.MIN_VALUE;
    }

    /**
     * 判断自己的timestamp是否可以通过
     * 
     * @throws InterruptedException
     */
    public void await(Event event) throws InterruptedException {
        long timestamp = getTimestamp(event);
        try {
            lock.lockInterruptibly();
            single(timestamp);
            while (isPermit(event, timestamp) == false) {
                condition.await();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 判断自己的timestamp是否可以通过,带超时控制
     * 
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void await(Event event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        long timestamp = getTimestamp(event);
        try {
            lock.lockInterruptibly();
            single(timestamp);
            while (isPermit(event, timestamp) == false) {
                condition.await(timeout, unit);
            }
        } finally {
            lock.unlock();
        }
    }

    public void clear(Event event) {
        // 出现中断有两种可能：
        // 1.出现主备切换，需要剔除到Timeline中的时间占位(这样合并时就会小于groupSize，不满足调度条件，直到主备切换完成后才能重新开启合并处理)
        // 2.出现关闭操作，退出即可
        lastTimestamps.remove(getTimestamp(event));
    }

    public void interrupt() {
        // do nothing，没有需要清理的上下文状态
    }

    public long state() {
        return threshold;
    }

    /**
     * 判断是否允许通过
     */
    protected boolean isPermit(Event event, long state) {
        return state <= state();
    }

    /**
     * 通知一下
     */
    protected void notify(long minTimestamp) {
        // 通知阻塞的线程恢复, 这里采用single all操作，当group中的几个时间都相同时，一次性触发通过多个
        condition.signalAll();
    }

    /**
     * 通知下一个minTimestamp数据出队列
     * 
     * @throws InterruptedException
     */
    private void single(long timestamp) throws InterruptedException {
        lastTimestamps.add(timestamp);

        if (timestamp < state()) {
            // 针对mysql事务中会出现时间跳跃
            // 例子：
            // 2012-08-08 16:24:26 事务头
            // 2012-08-08 16:24:24 变更记录
            // 2012-08-08 16:24:25 变更记录
            // 2012-08-08 16:24:26　事务尾

            // 针对这种case，一旦发现timestamp有回退的情况，直接更新threshold，强制阻塞其他的操作，等待最小数据优先处理完成
            threshold = timestamp; // 更新为最小值
        }

        if (lastTimestamps.size() >= groupSize) {// 判断队列是否需要触发
            // 触发下一个出队列的数据
            Long minTimestamp = this.lastTimestamps.peek();
            if (minTimestamp != null) {
                threshold = minTimestamp;
                notify(minTimestamp);
            }
        } else {
            threshold = Long.MIN_VALUE;// 如果不满足队列长度，需要阻塞等待
        }
    }

    private Long getTimestamp(Event event) {
        return event.getExecuteTime();
    }

}

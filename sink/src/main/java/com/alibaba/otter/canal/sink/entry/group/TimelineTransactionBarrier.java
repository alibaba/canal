package com.alibaba.otter.canal.sink.entry.group;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 相比于{@linkplain TimelineBarrier}，增加了按事务支持，会按照事务进行分库合并处理
 * 
 * @author jianghang 2012-10-18 下午05:18:38
 * @version 1.0.0
 */
public class TimelineTransactionBarrier extends TimelineBarrier {

    private ThreadLocal<Boolean> inTransaction = ThreadLocal.withInitial(() -> false);

    /**
     * <pre>
     * 几种状态：
     * 0：初始状态，允许大家竞争
     * 1: 事务数据处理中
     * 2: 非事务数据处理中
     * </pre>
     */
    private AtomicInteger        txState       = new AtomicInteger(0);

    public TimelineTransactionBarrier(int groupSize){
        super(groupSize);
    }

    public void await(Event event) throws InterruptedException {
        try {
            super.await(event);
        } catch (InterruptedException e) {
            // 出现线程中断，可能是因为关闭或者主备切换
            // 主备切换对应的事务尾会未正常发送，需要强制设置为事务结束，允许其他队列通过
            reset();
            throw e;
        }
    }

    public void await(Event event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        try {
            super.await(event, timeout, unit);
        } catch (InterruptedException e) {
            // 出现线程中断，可能是因为关闭或者主备切换
            // 主备切换对应的事务尾会未正常发送，需要强制设置为事务结束，允许其他队列通过
            reset();
            throw e;
        }
    }

    public void clear(Event event) {
        super.clear(event);

        // 应该先判断2，再判断是否是事务尾，因为事务尾也可以导致txState的状态为2
        // 如果先判断事务尾，那么2的状态可能永远没机会被修改了，系统出现死锁
        // CanalSinkException被注释的代码是不是可以放开？？我们内部使用的时候已经放开了，从代码逻辑的分析上以及实践效果来看，应该抛异常
        if (txState.intValue() == 2) {// 非事务中
            boolean result = txState.compareAndSet(2, 0);
            if (result == false) {
                throw new CanalSinkException("state is not correct in non-transaction");
            }
        } else if (isTransactionEnd(event)) {
            inTransaction.set(false); // 事务结束并且已经成功写入store，清理标记，进入重新排队判断，允许新的事务进入
            boolean result = txState.compareAndSet(1, 0);
            if (result == false) {
                throw new CanalSinkException("state is not correct in transaction");
            }
        }
    }

    protected boolean isPermit(Event event, long state) {
        if (txState.intValue() == 1 && inTransaction.get()) { // 如果处于事务中，直接允许通过。因为事务头已经做过判断
            return true;
        } else if (txState.intValue() == 0) {
            boolean result = super.isPermit(event, state);
            if (result) {
                // 可能第一条送过来的数据不为Begin，需要做判断处理，如果非事务，允许直接通过，比如DDL语句
                if (isTransactionBegin(event)) {
                    if (txState.compareAndSet(0, 1)) {
                        inTransaction.set(true);
                        return true; // 事务允许通过
                    }
                } else if (txState.compareAndSet(0, 2)) { // 非事务保护中
                    // 当基于zk-cursor启动的时候，拿到的第一个Event是TransactionEnd
                    return true; // DDL/DCL/TransactionEnd允许通过
                }
            }
        }

        return false;
    }

    public void interrupt() {
        super.interrupt();
        reset();
    }

    // 重新设置状态
    private void reset() {
        inTransaction.remove();
        txState.set(0);// 重新置位
    }

    private boolean isTransactionBegin(Event event) {
        return event.getEntryType() == EntryType.TRANSACTIONBEGIN;
    }

    private boolean isTransactionEnd(Event event) {
        return event.getEntryType() == EntryType.TRANSACTIONEND;
    }

}

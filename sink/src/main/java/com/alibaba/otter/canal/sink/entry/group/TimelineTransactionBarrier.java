package com.alibaba.otter.canal.sink.entry.group;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.erosa.protocol.protobuf.ErosaEntry.EntryType;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 相比于{@linkplain TimelineBarrier}，增加了按事务支持，会按照事务进行分库合并处理
 * 
 * @author jianghang 2012-10-18 下午05:18:38
 * @version 1.0.0
 */
public class TimelineTransactionBarrier extends TimelineBarrier {

    private ThreadLocal<Boolean> inTransaction = new ThreadLocal() {

                                                   protected Object initialValue() {
                                                       return false;
                                                   }
                                               };

    private AtomicBoolean        inPermit      = new AtomicBoolean(false);

    public TimelineTransactionBarrier(int groupSize){
        super(groupSize);
    }

    public void await(Event event) throws InterruptedException {
        try {
            super.await(event);
        } catch (InterruptedException e) {
            // 出现线程中断，可能是因为关闭或者主备切换
            // 主备切换对应的事务尾会未正常发送，需要强制设置为事务结束，允许其他队列通过
            if (inTransaction.get()) {
                inTransaction.remove();
                inPermit.compareAndSet(true, false);
            }
            throw e;
        }
    }

    public void await(Event event, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        try {
            super.await(event, timeout, unit);
        } catch (InterruptedException e) {
            // 出现线程中断，可能是因为关闭或者主备切换
            // 主备切换对应的事务尾会未正常发送，需要强制设置为事务结束，允许其他队列通过
            if (inTransaction.get()) {
                inTransaction.remove();
                inPermit.compareAndSet(true, false);
            }
            throw e;
        }
    }

    public void clear(Event event) {
        super.clear(event);

        if (isTransactionEnd(event)) {
            inTransaction.set(false); // 事务结束并且已经成功写入store，清理标记，进入重新排队判断，允许新的事务进入
            inPermit.compareAndSet(true, false);
        }
    }

    protected boolean isPermit(Event event, long state) {
        if (inTransaction.get()) { // 如果处于事务中，直接允许通过。因为事务头已经做过判断
            return true;
        } else {
            boolean result = super.isPermit(event, state);
            if (result && inPermit.compareAndSet(false, true)) { // 同一时间只能有一个在工作
                if (isTransactionBegin(event)) {
                    // 可能第一条送过来的数据不为Begin，需要做判断处理
                    inTransaction.set(true);
                }

                return result;
            } else {
                return false;
            }
        }
    }

    public void interrupt() {
        super.interrupt();
        // 清理状态，释放permit
        if (inTransaction.get()) {
            inTransaction.remove();
            inPermit.compareAndSet(true, false); // 尝试去关闭
        }
    }

    private boolean isTransactionBegin(Event event) {
        return event.getEntry().getEntryType() == EntryType.TRANSACTIONBEGIN;
    }

    private boolean isTransactionEnd(Event event) {
        return event.getEntry().getEntryType() == EntryType.TRANSACTIONEND;
    }

}

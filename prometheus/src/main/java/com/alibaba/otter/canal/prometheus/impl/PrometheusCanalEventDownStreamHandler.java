package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Chuanyi Li
 */
public class PrometheusCanalEventDownStreamHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    private final AtomicLong latestExecuteTime  = new AtomicLong(0L);
    private final AtomicLong transactionCounter = new AtomicLong(0L);
    private final AtomicLong rowEventCounter    = new AtomicLong(0L);
    private final AtomicLong rowsCounter        = new AtomicLong(0L);

    @Override
    public List<Event> before(List<Event> events) {
        long localExecTime = 0L;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                EntryType type = e.getEntryType();
                if (type == null) continue;
                long exec = e.getExecuteTime();
                if (exec > 0) localExecTime = exec;
                switch (type) {
                    case ROWDATA:
                        // TODO 当前proto无法直接获得荣威change的变更行数（需要parse），可考虑放到header里面
                        break;
                    case TRANSACTIONEND:
                        transactionCounter.incrementAndGet();
                        break;
                    case HEARTBEAT:
                        // TODO 确认一下不是canal自己产生的
                        // EventType eventType = e.getEventType();
                        // TODO utilize MySQL master heartbeat packet to refresh delay if always no more events coming
                        // see: https://dev.mysql.com/worklog/task/?id=342
                        // heartbeats are sent by the master only if there is no
                        // more unsent events in the actual binlog file for a period longer that
                        // master_heartbeat_period.
                        break;
                    default:
                        break;
                }
            }
            if (localExecTime > 0) {
                latestExecuteTime.lazySet(localExecTime);
            }
        }
        return events;
    }

    @Override
    public void start() {

        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    public AtomicLong getLatestExecuteTime() {
        return latestExecuteTime;
    }

    public AtomicLong getTransactionCounter() {
        return transactionCounter;
    }

    public AtomicLong getRowsCounter() {
        return rowsCounter;
    }

    public AtomicLong getRowEventCounter() {
        return rowEventCounter;
    }
}

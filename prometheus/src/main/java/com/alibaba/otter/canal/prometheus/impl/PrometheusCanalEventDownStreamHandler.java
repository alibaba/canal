package com.alibaba.otter.canal.prometheus.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;

/**
 * @author Chuanyi Li
 */
public class PrometheusCanalEventDownStreamHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    private final AtomicLong latestExecuteTime  = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong transactionCounter = new AtomicLong(0L);

    @Override
    public List<Event> before(List<Event> events) {
        long localExecTime = 0L;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                EntryType type = e.getEntryType();
                if (type == null) continue;
                switch (type) {
                    case TRANSACTIONBEGIN: {
                        long exec = e.getExecuteTime();
                        if (exec > 0) {
                            localExecTime = exec;
                        }
                        break;
                    }
                    case ROWDATA: {
                        long exec = e.getExecuteTime();
                        if (exec > 0) {
                            localExecTime = exec;
                        }
                        break;
                    }
                    case TRANSACTIONEND: {
                        long exec = e.getExecuteTime();
                        if (exec > 0) {
                            localExecTime = exec;
                        }
                        transactionCounter.incrementAndGet();
                        break;
                    }
                    case HEARTBEAT:
                        CanalEntry.EventType eventType = e.getEventType();
                        if (eventType == CanalEntry.EventType.MHEARTBEAT) {
                            localExecTime = System.currentTimeMillis();
                        }
                        break;
                    default:
                        break;
                }
            }
            if (localExecTime > 0) {
                latestExecuteTime.set(localExecTime);
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

}

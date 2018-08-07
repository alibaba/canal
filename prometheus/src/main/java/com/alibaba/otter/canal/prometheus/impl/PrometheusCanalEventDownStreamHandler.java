package com.alibaba.otter.canal.prometheus.impl;

import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Chuanyi Li
 */
public class PrometheusCanalEventDownStreamHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    private final AtomicLong    latestExecuteTime = new AtomicLong(0L);

    @Override
    public List<Event> before(List<Event> events) {
        // TODO utilize MySQL master heartbeat packet to refresh delay if always no more events coming
        // see: https://dev.mysql.com/worklog/task/?id=342
        // heartbeats are sent by the master only if there is no
        // more unsent events in the actual binlog file for a period longer that
        // master_heartbeat_period.
        if (events != null && !events.isEmpty()) {
            Event last = events.get(events.size() - 1);
            long ts = last.getExecuteTime();
            long ls = latestExecuteTime.get();
            if (ts > ls) {
                latestExecuteTime.lazySet(ts);
            }
        }
        return events;
    }

    public AtomicLong getLatestExecuteTime() {
        return latestExecuteTime;
    }

}

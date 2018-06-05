package com.alibaba.otter.canal.sink.mongo;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.alibaba.otter.canal.sink.mongo.handler.CustomEventHandler;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * mongo sink
 * @author dsqin
 * @date 2018/5/17
 */
public class MongoEventSink extends AbstractCanalEventSink<List<CanalEntry.Entry>> implements CanalEventSink<List<CanalEntry.Entry>> {

    private static final Logger logger                        = LoggerFactory.getLogger(MongoEventSink.class);
    private static final int       maxFullTimes                  = 10;
    private CanalEventStore<Event> eventStore;
    protected boolean              filterTransactionEntry        = false;                                        // 是否需要过滤事务头/尾
    protected boolean              filterEmtryTransactionEntry   = true;                                         // 是否需要过滤空的事务头/尾
    protected long                 emptyTransactionInterval      = 5 * 1000;                                     // 空的事务输出的频率
    protected long                 emptyTransctionThresold       = 8192;                                         // 超过1024个事务头，输出一个
    protected volatile long        lastEmptyTransactionTimestamp = 0L;
    protected AtomicLong lastEmptyTransactionCount     = new AtomicLong(0L);


    public MongoEventSink() {
        addHandler(new CustomEventHandler());
    }

    @Override
    public void start() {
        super.start();
        Assert.notNull(eventStore);

        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (!handler.isStart()) {
                handler.start();
            }
        }
    }

    @Override
    public void stop() {
        super.stop();

        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (handler.isStart()) {
                handler.stop();
            }
        }
    }

    @Override
    public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination) throws CanalSinkException, InterruptedException {

        List<Event> events = Lists.newArrayList();
        for (CanalEntry.Entry entry : entrys) {
            Event event = new Event(new LogIdentity(remoteAddress, -1L), entry);
            if (!doFilter(event)) {
                continue;
            }

            events.add(event);
        }

        return doSink(events);
    }

    protected boolean doFilter(Event event) {
        if (filter != null && event.getEntry().getEntryType() == CanalEntry.EntryType.ROWDATA) {
            String name = "";
            boolean need = filter.filter(name);
            if (!need) {
                logger.debug("filter name[{}] entry : {}:{}",
                        name,
                        event.getEntry().getHeader().getLogfileName(),
                        event.getEntry().getHeader().getLogfileOffset());
            }

            return need;
        } else {
            return true;
        }
    }

    protected boolean doSink(List<Event> events) {
        for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
            events = handler.before(events);
        }

        int fullTimes = 0;
        do {
            boolean success = eventStore.tryPut(events);
            if (success) {
                for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                    events = handler.after(events);
                }
                return true;
            } else {
                applyWait(++fullTimes);
            }

            for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                events = handler.retry(events);
            }

        } while (running && !Thread.interrupted());
        return false;
    }

    // 处理无数据的情况，避免空循环挂死
    private void applyWait(int fullTimes) {
        int newFullTimes = fullTimes > maxFullTimes ? maxFullTimes : fullTimes;
        if (fullTimes <= 3) { // 3次以内
            Thread.yield();
        } else { // 超过3次，最多只sleep 10ms
            LockSupport.parkNanos(1000 * 1000L * newFullTimes);
        }

    }


    public CanalEventStore<Event> getEventStore() {
        return eventStore;
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public boolean isFilterTransactionEntry() {
        return filterTransactionEntry;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public boolean isFilterEmtryTransactionEntry() {
        return filterEmtryTransactionEntry;
    }

    public void setFilterEmtryTransactionEntry(boolean filterEmtryTransactionEntry) {
        this.filterEmtryTransactionEntry = filterEmtryTransactionEntry;
    }

    public long getEmptyTransactionInterval() {
        return emptyTransactionInterval;
    }

    public void setEmptyTransactionInterval(long emptyTransactionInterval) {
        this.emptyTransactionInterval = emptyTransactionInterval;
    }

    public long getEmptyTransctionThresold() {
        return emptyTransctionThresold;
    }

    public void setEmptyTransctionThresold(long emptyTransctionThresold) {
        this.emptyTransctionThresold = emptyTransctionThresold;
    }
}

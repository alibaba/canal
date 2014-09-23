package com.alibaba.otter.canal.parse.inbound.group;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

public class DummyEventStore implements CanalEventStore<Event> {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String messgae     = "{0} [{1}:{2}:{3}] {4} {5}.{6}";

    public void ack(Position position) throws CanalStoreException {

    }

    public Events get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        return null;
    }

    public Events get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException,
                                                                                 CanalStoreException {
        return null;
    }

    public Position getFirstPosition() throws CanalStoreException {
        return null;
    }

    public Position getLatestPosition() throws CanalStoreException {
        return null;
    }

    public void rollback() throws CanalStoreException {

    }

    public Events tryGet(Position start, int batchSize) throws CanalStoreException {
        return null;
    }

    public boolean isStart() {
        return false;
    }

    public void start() {

    }

    public void stop() {

    }

    public void cleanAll() throws CanalStoreException {
    }

    public void cleanUntil(Position position) throws CanalStoreException {

    }

    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Arrays.asList(data));
    }

    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Arrays.asList(data), timeout, unit);
    }

    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Arrays.asList(data));
    }

    public void put(List<Event> datas) throws InterruptedException, CanalStoreException {
        for (Event data : datas) {
            CanalEntry.Header header = data.getEntry().getHeader();
            Date date = new Date(header.getExecuteTime());
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
            if (data.getEntry().getEntryType() == EntryType.TRANSACTIONBEGIN
                || data.getEntry().getEntryType() == EntryType.TRANSACTIONEND) {
                // System.out.println(MessageFormat.format(messgae, new Object[] { Thread.currentThread().getName(),
                // header.getLogfilename(), header.getLogfileoffset(), format.format(date),
                // data.getEntry().getEntryType(), "" }));
                System.out.println(data.getEntry().getEntryType());

            } else {
                System.out.println(MessageFormat.format(messgae,
                                                        new Object[] { Thread.currentThread().getName(),
                                                                header.getLogfileName(),
                                                                String.valueOf(header.getLogfileOffset()),
                                                                format.format(date), header.getEventType(),
                                                                header.getSchemaName(), header.getTableName() }));
            }
        }
    }

    public boolean put(List<Event> datas, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        for (Event data : datas) {
            CanalEntry.Header header = data.getEntry().getHeader();
            Date date = new Date(header.getExecuteTime());
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
            if (data.getEntry().getEntryType() == EntryType.TRANSACTIONBEGIN
                || data.getEntry().getEntryType() == EntryType.TRANSACTIONEND) {
                // System.out.println(MessageFormat.format(messgae, new Object[] { Thread.currentThread().getName(),
                // header.getLogfilename(), header.getLogfileoffset(), format.format(date),
                // data.getEntry().getEntryType(), "" }));
                System.out.println(data.getEntry().getEntryType());

            } else {
                System.out.println(MessageFormat.format(messgae,
                                                        new Object[] { Thread.currentThread().getName(),
                                                                header.getLogfileName(),
                                                                String.valueOf(header.getLogfileOffset()),
                                                                format.format(date), header.getEventType(),
                                                                header.getSchemaName(), header.getTableName() }));
            }
        }
        return true;
    }

    public boolean tryPut(List<Event> datas) throws CanalStoreException {
        System.out.println("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        for (Event data : datas) {

            CanalEntry.Header header = data.getEntry().getHeader();
            Date date = new Date(header.getExecuteTime());
            SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
            if (data.getEntry().getEntryType() == EntryType.TRANSACTIONBEGIN
                || data.getEntry().getEntryType() == EntryType.TRANSACTIONEND) {
                // System.out.println(MessageFormat.format(messgae, new Object[] { Thread.currentThread().getName(),
                // header.getLogfilename(), header.getLogfileoffset(), format.format(date),
                // data.getEntry().getEntryType(), "" }));
                System.out.println(data.getEntry().getEntryType());

            } else {
                System.out.println(MessageFormat.format(messgae,
                                                        new Object[] { Thread.currentThread().getName(),
                                                                header.getLogfileName(),
                                                                String.valueOf(header.getLogfileOffset()),
                                                                format.format(date), header.getEventType(),
                                                                header.getSchemaName(), header.getTableName() }));
            }

        }
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
        return true;
    }

}

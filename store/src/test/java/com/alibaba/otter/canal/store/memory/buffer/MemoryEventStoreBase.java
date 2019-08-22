package com.alibaba.otter.canal.store.memory.buffer;

import java.net.InetSocketAddress;

import org.junit.Assert;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.store.model.Event;

public class MemoryEventStoreBase {

    private static final String MYSQL_ADDRESS = "127.0.0.1";

    protected void sleep(Long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Assert.fail();
        }
    }

    protected Event buildEvent(String binlogFile, long offset, long timestamp) {
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(offset);
        headerBuilder.setExecuteTime(timestamp);
        headerBuilder.setEventLength(1024);
        Entry.Builder entryBuilder = Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        Entry entry = entryBuilder.build();

        return new Event(new LogIdentity(new InetSocketAddress(MYSQL_ADDRESS, 3306), 1234L), entry);
    }

    protected Event buildEvent(String binlogFile, long offset, long timestamp, long eventLenght) {
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(offset);
        headerBuilder.setExecuteTime(timestamp);
        headerBuilder.setEventLength(eventLenght);
        Entry.Builder entryBuilder = Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        Entry entry = entryBuilder.build();

        return new Event(new LogIdentity(new InetSocketAddress(MYSQL_ADDRESS, 3306), 1234L), entry);
    }
}

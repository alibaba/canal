package com.alibaba.otter.canal.parse.inbound;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;

public class EventTransactionBufferTest {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String messgae     = "{0} [{1}:{2}:{3}] {4}.{5}";

    @Test
    public void testTransactionFlush() {
        final int bufferSize = 64;
        final int transactionSize = 5;
        EventTransactionBuffer buffer = new EventTransactionBuffer();
        buffer.setBufferSize(bufferSize);
        buffer.setFlushCallback(transaction -> {
            Assert.assertEquals(transactionSize, transaction.size());
            System.out.println("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            for (Entry data : transaction) {

                Header header = data.getHeader();
                Date date = new Date(header.getExecuteTime());
                SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
                if (data.getEntryType() == EntryType.TRANSACTIONBEGIN
                    || data.getEntryType() == EntryType.TRANSACTIONEND) {
                    System.out.println(data.getEntryType());

                } else {
                    System.out.println(MessageFormat.format(messgae, new Object[] {
                            Thread.currentThread().getName(), header.getLogfileName(), header.getLogfileOffset(),
                            format.format(date), header.getSchemaName(), header.getTableName() }));
                }

            }
            System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
        });
        buffer.start();

        try {
            for (int i = 0; i < transactionSize * 10; i++) {
                if (i % transactionSize == 0) {
                    buffer.add(buildEntry("1", 1L + i, 40L + i, EntryType.TRANSACTIONBEGIN));
                } else if ((i + 1) % transactionSize == 0) {
                    buffer.add(buildEntry("1", 1L + i, 40L + i, EntryType.TRANSACTIONEND));
                } else {
                    buffer.add(buildEntry("1", 1L + i, 40L + i));
                }
            }
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        buffer.stop();
    }

    @Test
    public void testForceFlush() {
        final int bufferSize = 64;
        EventTransactionBuffer buffer = new EventTransactionBuffer();
        buffer.setBufferSize(bufferSize);
        buffer.setFlushCallback(transaction -> {
            Assert.assertEquals(bufferSize, transaction.size());
            System.out.println("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            for (Entry data : transaction) {

                Header header = data.getHeader();
                Date date = new Date(header.getExecuteTime());
                SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
                if (data.getEntryType() == EntryType.TRANSACTIONBEGIN
                    || data.getEntryType() == EntryType.TRANSACTIONEND) {
                    // System.out.println(MessageFormat.format(messgae, new
                    // Object[] {
                    // Thread.currentThread().getName(),
                    // header.getLogfilename(), header.getLogfileoffset(),
                    // format.format(date),
                    // data.getEntry().getEntryType(), "" }));
                    System.out.println(data.getEntryType());

                } else {
                    System.out.println(MessageFormat.format(messgae, new Object[] {
                            Thread.currentThread().getName(), header.getLogfileName(), header.getLogfileOffset(),
                            format.format(date), header.getSchemaName(), header.getTableName() }));
                }

            }
            System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
        });
        buffer.start();

        try {
            for (int i = 0; i < bufferSize * 2 + 1; i++) {
                buffer.add(buildEntry("1", 1L + i, 40L + i));
            }
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        buffer.stop();
    }

    private static Entry buildEntry(String binlogFile, long offset, long timestamp) {
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(offset);
        headerBuilder.setExecuteTime(timestamp);
        Entry.Builder entryBuilder = Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        return entryBuilder.build();
    }

    private static Entry buildEntry(String binlogFile, long offset, long timestamp, EntryType type) {
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(offset);
        headerBuilder.setExecuteTime(timestamp);
        Entry.Builder entryBuilder = Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        entryBuilder.setEntryType(type);
        return entryBuilder.build();
    }
}

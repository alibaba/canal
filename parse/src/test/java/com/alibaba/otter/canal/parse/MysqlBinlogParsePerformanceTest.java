package com.alibaba.otter.canal.parse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer;
import com.taobao.tddl.dbsync.binlog.event.RowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent.ColumnInfo;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import org.junit.Ignore;

@Ignore
public class MysqlBinlogParsePerformanceTest {

    protected static Charset charset = Charset.forName("utf-8");

    public static void main(String args[]) {
        try (DirectLogFetcher fetcher = new DirectLogFetcher()) {
            MysqlConnector connector = new MysqlConnector(new InetSocketAddress("127.0.0.1", 3306), "root", "hello");
            connector.connect();
            updateSettings(connector);
            sendBinlogDump(connector, "mysql-bin.000006", 120L, 3);
            fetcher.start(connector.getChannel());
            final BlockingQueue<LogBuffer> buffer = new ArrayBlockingQueue<>(1024);
            Thread thread = new Thread(() -> {
                try {
                    consumer(buffer);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
            thread.start();

            while (fetcher.fetch()) {
                buffer.put(fetcher.duplicate());
                fetcher.consume(fetcher.limit());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void consumer(BlockingQueue<LogBuffer> buffer) throws IOException, InterruptedException {
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();

        AtomicLong sum = new AtomicLong(0);
        long start = System.currentTimeMillis();
        long last = 0;
        long end = 0;
        while (true) {
            LogEvent event = null;
            event = decoder.decode(buffer.take(), context);
            int eventType = event.getHeader().getType();
            switch (eventType) {
                case LogEvent.ROTATE_EVENT:
                    break;
                case LogEvent.WRITE_ROWS_EVENT_V1:
                case LogEvent.WRITE_ROWS_EVENT:
                    parseRowsEvent((WriteRowsLogEvent) event, sum);
                    break;
                case LogEvent.UPDATE_ROWS_EVENT_V1:
                case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
                case LogEvent.UPDATE_ROWS_EVENT:
                    parseRowsEvent((UpdateRowsLogEvent) event, sum);
                    break;
                case LogEvent.DELETE_ROWS_EVENT_V1:
                case LogEvent.DELETE_ROWS_EVENT:
                    parseRowsEvent((DeleteRowsLogEvent) event, sum);
                    break;
                case LogEvent.XID_EVENT:
                    sum.incrementAndGet();
                    break;
                case LogEvent.QUERY_EVENT:
                    sum.incrementAndGet();
                    break;
                default:
                    break;
            }

            long current = sum.get();
            if (current - last >= 100000) {
                end = System.currentTimeMillis();
                long tps = ((current - last) * 1000) / (end - start);
                System.out.println(" total : " + sum + " , cost : " + (end - start) + " , tps : " + tps);
                last = current;
                start = end;
            }
        }
    }

    private static void sendBinlogDump(MysqlConnector connector, String binlogfilename, Long binlogPosition, int slaveId)
                                                                                                                         throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), binlogDumpHeader.toBytes(), cmdBody);
    }

    private static void updateSettings(MysqlConnector connector) throws IOException {
        update("set @master_binlog_checksum= '@@global.binlog_checksum'", connector);
        update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'", connector);
    }

    public static void update(String cmd, MysqlConnector connector) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    public static void parseRowsEvent(RowsLogEvent event, AtomicLong sum) {
        try {
            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getChangeColumns();
            while (buffer.nextOneRow(columns)) {
                int type = event.getHeader().getType();
                if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
                    parseOneRow(event, buffer, columns, true);
                } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
                    parseOneRow(event, buffer, columns, false);
                } else {
                    parseOneRow(event, buffer, columns, false);
                    if (!buffer.nextOneRow(changeColumns, true)) {
                        break;
                    }
                    parseOneRow(event, buffer, changeColumns, true);
                }

                sum.incrementAndGet();
            }
        } catch (Exception e) {
            throw new RuntimeException("parse row data failed.", e);
        }
    }

    public static void parseOneRow(RowsLogEvent event, RowsLogBuffer buffer, BitSet cols, boolean isAfter)
                                                                                                          throws UnsupportedEncodingException {
        TableMapLogEvent map = event.getTable();
        if (map == null) {
            throw new RuntimeException("not found TableMap with tid=" + event.getTableId());
        }

        final int columnCnt = map.getColumnCnt();
        final ColumnInfo[] columnInfo = map.getColumnInfo();
        for (int i = 0; i < columnCnt; i++) {
            if (!cols.get(i)) {
                continue;
            }

            ColumnInfo info = columnInfo[i];
            buffer.nextValue(null, i, info.type, info.meta);
            if (buffer.isNull()) {
            } else {
                buffer.getValue();
            }
        }
    }
}

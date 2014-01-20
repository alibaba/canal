package com.alibaba.otter.canal.parse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;

public class DirectLogFetcherTest {

    @Test
    public void testSimple() {
        DirectLogFetcher fetcher = new DirectLogFetcher();
        try {
            // Class.forName("com.mysql.jdbc.Driver");
            // Connection connection =
            // DriverManager.getConnection("jdbc:mysql://10.20.144.29:3306",
            // "ottermysql",
            // "ottermysql");
            // Statement statement = connection.createStatement();
            // statement.execute("SET @master_binlog_checksum='@@global.binlog_checksum'");

            MysqlConnector connector = new MysqlConnector(new InetSocketAddress("10.20.144.29", 3306),
                "ottermysql",
                "ottermysql");
            connector.connect();
            sendBinlogDump(connector, "mysql-bin.001016", 4L, 3);

            fetcher.start(connector.getChannel());

            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            while (fetcher.fetch()) {
                LogEvent event = null;
                event = decoder.decode(fetcher, context);

                if (event == null) {
                    throw new RuntimeException("parse failed");
                }

                int eventType = event.getHeader().getType();
                switch (eventType) {
                    case LogEvent.ROTATE_EVENT:
                        // binlogFileName = ((RotateLogEvent)
                        // event).getFilename();
                        break;
                    case LogEvent.WRITE_ROWS_EVENT_V1:
                    case LogEvent.WRITE_ROWS_EVENT:
                        // parseRowsEvent((WriteRowsLogEvent) event);
                        break;
                    case LogEvent.UPDATE_ROWS_EVENT_V1:
                    case LogEvent.UPDATE_ROWS_EVENT:
                        // parseRowsEvent((UpdateRowsLogEvent) event);
                        break;
                    case LogEvent.DELETE_ROWS_EVENT_V1:
                    case LogEvent.DELETE_ROWS_EVENT:
                        // parseRowsEvent((DeleteRowsLogEvent) event);
                        break;
                    case LogEvent.QUERY_EVENT:
                        // parseQueryEvent((QueryLogEvent) event);
                        break;
                    case LogEvent.ROWS_QUERY_LOG_EVENT:
                        // parseRowsQueryEvent((RowsQueryLogEvent) event);
                        break;
                    case LogEvent.ANNOTATE_ROWS_EVENT:
                        break;
                    case LogEvent.XID_EVENT:
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            try {
                fetcher.close();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }

    }

    private void sendBinlogDump(MysqlConnector connector, String binlogfilename, Long binlogPosition, int slaveId)
                                                                                                                  throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(connector.getChannel(), new ByteBuffer[] { ByteBuffer.wrap(binlogDumpHeader.toBytes()),
                ByteBuffer.wrap(cmdBody) });
    }
}

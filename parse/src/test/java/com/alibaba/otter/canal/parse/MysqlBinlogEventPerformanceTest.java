package com.alibaba.otter.canal.parse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import org.junit.Ignore;

@Ignore
public class MysqlBinlogEventPerformanceTest {

    protected static Charset charset = Charset.forName("utf-8");

    public static void main(String args[]) {
        try (DirectLogFetcher fetcher = new DirectLogFetcher()) {
            MysqlConnector connector = new MysqlConnector(new InetSocketAddress("127.0.0.1", 3306), "root", "hello");
            connector.connect();
            updateSettings(connector);
            sendBinlogDump(connector, "mysql-bin.000006", 120L, 3);
            fetcher.start(connector.getChannel());
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            AtomicLong sum = new AtomicLong(0);
            long start = System.currentTimeMillis();
            long last = 0;
            long end = 0;
            while (fetcher.fetch()) {
                decoder.decode(fetcher, context);
                sum.incrementAndGet();
                long current = sum.get();
                if (current - last >= 100000) {
                    end = System.currentTimeMillis();
                    long tps = ((current - last) * 1000) / (end - start);
                    System.out.println(" total : " + sum + " , cost : " + (end - start) + " , tps : " + tps);
                    last = current;
                    start = end;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
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

}

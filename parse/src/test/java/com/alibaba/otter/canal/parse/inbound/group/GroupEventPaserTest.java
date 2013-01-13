package com.alibaba.otter.canal.parse.inbound.group;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Test;

import com.alibaba.erosa.parse.DefaultMysqlBinlogParser;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry.Entry;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.stub.AbstractCanalLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.sink.entry.group.GroupEventSink;

public class GroupEventPaserTest {

    private static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    private static final String MYSQL_ADDRESS = "10.20.153.51";
    private static final String USERNAME      = "retl";
    private static final String PASSWORD      = "retl";

    @Test
    public void testMysqlWithMysql() {
        // MemoryEventStoreWithBuffer eventStore = new MemoryEventStoreWithBuffer();
        // eventStore.setBufferSize(8196);

        GroupEventSink eventSink = new GroupEventSink(3);
        eventSink.setFilterTransactionEntry(false);
        eventSink.setEventStore(new DummyEventStore());
        eventSink.start();

        // 构造第一个mysql
        MysqlEventParser mysqlEventPaser1 = buildEventParser(3344);
        mysqlEventPaser1.setEventSink(eventSink);
        // 构造第二个mysql
        MysqlEventParser mysqlEventPaser2 = buildEventParser(3345);
        mysqlEventPaser2.setEventSink(eventSink);
        // 构造第二个mysql
        MysqlEventParser mysqlEventPaser3 = buildEventParser(3346);
        mysqlEventPaser3.setEventSink(eventSink);
        // 启动
        mysqlEventPaser1.start();
        mysqlEventPaser2.start();
        mysqlEventPaser3.start();

        try {
            Thread.sleep(30 * 10 * 1000L);
        } catch (InterruptedException e) {
        }

        mysqlEventPaser1.stop();
        mysqlEventPaser2.stop();
        mysqlEventPaser3.stop();
    }

    private MysqlEventParser buildEventParser(int slaveId) {
        MysqlEventParser mysqlEventPaser = new MysqlEventParser();
        EntryPosition defaultPosition = buildPosition("mysql-bin.000001", 6163L, 1322803601000L);
        mysqlEventPaser.setDestination("group-" + slaveId);
        mysqlEventPaser.setSlaveId(slaveId);
        mysqlEventPaser.setDetectingEnable(false);
        mysqlEventPaser.setDetectingSQL(DETECTING_SQL);
        mysqlEventPaser.setMasterInfo(buildAuthentication());
        mysqlEventPaser.setMasterPosition(defaultPosition);
        mysqlEventPaser.setBinlogParser(buildParser(buildAuthentication()));
        mysqlEventPaser.setEventSink(new EntryEventSink());
        mysqlEventPaser.setLogPositionManager(new AbstractCanalLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                // System.out.println(logPosition);
            }

            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }
        });
        return mysqlEventPaser;
    }

    private BinlogParser buildParser(AuthenticationInfo info) {
        final DefaultMysqlBinlogParser _parser = new DefaultMysqlBinlogParser(Charset.forName("UTF-8"),
                                                                              (InetSocketAddress) info.getAddress(),
                                                                              info.getUsername(), info.getPassword());

        return new AbstractBinlogParser() {

            public List<Entry> parse(byte[] event) throws CanalParseException {
                return _parser.parse(event);
            }
        };
    }

    private EntryPosition buildPosition(String binlogFile, Long offest, Long timestamp) {
        return new EntryPosition(binlogFile, offest, timestamp);
    }

    private AuthenticationInfo buildAuthentication() {
        return new AuthenticationInfo(new InetSocketAddress(MYSQL_ADDRESS, 3306), USERNAME, PASSWORD);
    }
}

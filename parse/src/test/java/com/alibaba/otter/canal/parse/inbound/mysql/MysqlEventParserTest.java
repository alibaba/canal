package com.alibaba.otter.canal.parse.inbound.mysql;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.erosa.parse.DefaultMysqlBinlogParser;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry.Entry;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry.EntryType;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.stub.AbstractCanalLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;

public class MysqlEventParserTest {

    private static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    private static final String MYSQL_ADDRESS = "10.20.153.51";
    private static final String USERNAME      = "retl";
    private static final String PASSWORD      = "retl";

    @Test
    public void test_position() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker();
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition defaultPosition = buildPosition("mysql-bin.000001", 6163L, 1322803601000L);

        controller.setSlaveId(3344L);
        controller.setDetectingEnable(true);
        controller.setDetectingSQL(DETECTING_SQL);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setBinlogParser(buildParser(buildAuthentication()));
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    entryCount.incrementAndGet();

                    if (!(entry.getEntryType() == EntryType.MYSQL_FORMATDESCRIPTION || entry.getEntryType() == EntryType.MYSQL_ROTATE)) {
                        String logfilename = entry.getHeader().getLogfilename();
                        long logfileoffset = entry.getHeader().getLogfileoffset();
                        long executeTime = entry.getHeader().getExecutetime();
                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);

                        controller.stop();
                        timeoutChecker.stop();
                    }
                }
                timeoutChecker.touch();
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractCanalLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                System.out.println(logPosition);
            }

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }
        });

        controller.setHeartBeatCallback(new HeartBeatCallback() {

            public void onSuccess(long costTime) {
            }

            public void onFailed(Exception e) {
                e.printStackTrace();
            }
        });

        controller.start();

        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        Assert.assertEquals(entryPosition, defaultPosition);
    }

    @Test
    public void test_timestamp() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(30 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition defaultPosition = buildPosition(null, null, 1322803601000L);
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(true);
        controller.setDetectingSQL(DETECTING_SQL);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setBinlogParser(buildParser(buildAuthentication()));
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    entryCount.incrementAndGet();

                    if (!(entry.getEntryType() == EntryType.MYSQL_FORMATDESCRIPTION || entry.getEntryType() == EntryType.MYSQL_ROTATE)) {
                        String logfilename = entry.getHeader().getLogfilename();
                        long logfileoffset = entry.getHeader().getLogfileoffset();
                        long executeTime = entry.getHeader().getExecutetime();

                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);

                        controller.stop();
                        timeoutChecker.stop();
                    }
                }
                timeoutChecker.touch();
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractCanalLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                System.out.println(logPosition);
            }

            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }
        });

        controller.setHeartBeatCallback(new HeartBeatCallback() {

            public void onSuccess(long costTime) {
            }

            public void onFailed(Exception e) {
                e.printStackTrace();
            }
        });

        controller.start();
        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        assertEquals(entryPosition.getJournalName(), "mysql-bin.000001");
        assertTrue(entryPosition.getPosition() <= 6163L);
        assertTrue(entryPosition.getTimestamp() <= defaultPosition.getTimestamp());
    }

    @Test
    public void test_ha() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(30 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition defaultPosition = buildPosition("mysql-bin.000001", 6163L, 1322803601000L);
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setBinlogParser(buildParser(buildAuthentication()));
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    entryCount.incrementAndGet();

                    if (!(entry.getEntryType() == EntryType.MYSQL_FORMATDESCRIPTION || entry.getEntryType() == EntryType.MYSQL_ROTATE)) {
                        String logfilename = entry.getHeader().getLogfilename();
                        long logfileoffset = entry.getHeader().getLogfileoffset();
                        long executeTime = entry.getHeader().getExecutetime();

                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);

                        controller.stop();
                        timeoutChecker.stop();
                    }
                }
                timeoutChecker.touch();
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractCanalLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                System.out.println(logPosition);
            }

            public LogPosition getLatestIndexBy(String destination) {
                LogPosition masterLogPosition = new LogPosition();
                masterLogPosition.setIdentity(new LogIdentity(new InetSocketAddress("10.20.153.53", 3306), 1234L));
                masterLogPosition.setPostion(new EntryPosition(1322803601000L));
                return masterLogPosition;
            }
        });

        controller.setHeartBeatCallback(new HeartBeatCallback() {

            public void onSuccess(long costTime) {
            }

            public void onFailed(Exception e) {
                e.printStackTrace();
            }
        });

        controller.start();
        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        Assert.assertEquals(entryPosition.getJournalName(), "mysql-bin.000001");
        assertTrue(entryPosition.getPosition() <= 6163L);
        assertTrue(entryPosition.getTimestamp() <= defaultPosition.getTimestamp());
    }

    @Test
    public void test_no_position() throws InterruptedException { // 在某个文件下，找不到对应的timestamp数据，会使用106L position进行数据抓取
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3 * 60 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition defaultPosition = buildPosition("mysql-bin.000001", null,
                                                            new Date().getTime() + 1000 * 1000L);
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setBinlogParser(buildParser(buildAuthentication()));
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    entryCount.incrementAndGet();

                    if (!(entry.getEntryType() == EntryType.MYSQL_FORMATDESCRIPTION || entry.getEntryType() == EntryType.MYSQL_ROTATE)) {
                        // String logfilename = entry.getHeader().getLogfilename();
                        // long logfileoffset = entry.getHeader().getLogfileoffset();
                        long executeTime = entry.getHeader().getExecutetime();

                        // entryPosition.setJournalName(logfilename);
                        // entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);

                        controller.stop();
                        timeoutChecker.stop();
                    }
                }
                timeoutChecker.touch();
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractCanalLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                System.out.println(logPosition);
            }

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }
        });

        controller.setHeartBeatCallback(new HeartBeatCallback() {

            public void onSuccess(long costTime) {
            }

            public void onFailed(Exception e) {
                e.printStackTrace();
            }
        });

        controller.start();
        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        // Assert.assertEquals(logfilename, "mysql-bin.000001");
        // Assert.assertEquals(106L, logfileoffset);
        assertTrue(entryPosition.getTimestamp() < defaultPosition.getTimestamp());
    }

    // ======================== helper method =======================

    private BinlogParser buildParser(AuthenticationInfo info) {
        final DefaultMysqlBinlogParser _parser = new DefaultMysqlBinlogParser(Charset.forName("UTF-8"),
                                                                              (InetSocketAddress) info.getAddress(),
                                                                              info.getUsername(), info.getPassword());

        return new AbstractBinlogParser() {

            @Override
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

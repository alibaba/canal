package com.alibaba.otter.canal.parse.inbound.mysql;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
@Ignore
public class MysqlEventParserTest {

    private static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    private static final String MYSQL_ADDRESS = "127.0.0.1";
    private static final String USERNAME      = "canal";
    private static final String PASSWORD      = "canal";

    @Test
    public void test_position() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker();
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition defaultPosition = buildPosition("mysql-bin.000003", 4690L, 1505481064000L);

        controller.setSlaveId(3344L);
        controller.setDetectingEnable(true);
        controller.setDetectingSQL(DETECTING_SQL);
        controller.setMasterPosition(defaultPosition);
        controller.setMasterInfo(buildAuthentication());
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    if (entry.getEntryType() != EntryType.HEARTBEAT) {
                        entryCount.incrementAndGet();
                        String logfilename = entry.getHeader().getLogfileName();
                        long logfileoffset = entry.getHeader().getLogfileOffset();
                        long executeTime = entry.getHeader().getExecuteTime();
                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);
                        break;
                    }
                }

                if (entryCount.get() > 0) {
                    controller.stop();
                    timeoutChecker.stop();
                    timeoutChecker.touch();
                }
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractLogPositionManager() {

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }

            @Override
            public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
                System.out.println(logPosition);
            }
        });

        controller.start();

        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        Assert.assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        Assert.assertEquals(entryPosition, defaultPosition);
    }

    @Test
    public void test_timestamp() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3000 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition defaultPosition = buildPosition(null, null, 1475116855000L);
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setDetectingSQL(DETECTING_SQL);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    if (entry.getEntryType() != EntryType.HEARTBEAT) {
                        entryCount.incrementAndGet();

                        String logfilename = entry.getHeader().getLogfileName();
                        long logfileoffset = entry.getHeader().getLogfileOffset();
                        long executeTime = entry.getHeader().getExecuteTime();

                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);
                        break;
                    }
                }

                if (entryCount.get() > 0) {
                    controller.stop();
                    timeoutChecker.stop();
                    timeoutChecker.touch();
                }
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                System.out.println(logPosition);
            }

            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }
        });

        controller.start();
        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        Assert.assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        Assert.assertEquals(entryPosition.getJournalName(), "mysql-bin.000001");
        Assert.assertTrue(entryPosition.getPosition() <= 6163L);
        Assert.assertTrue(entryPosition.getTimestamp() <= defaultPosition.getTimestamp());
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
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    if (entry.getEntryType() != EntryType.HEARTBEAT) {

                        entryCount.incrementAndGet();

                        String logfilename = entry.getHeader().getLogfileName();
                        long logfileoffset = entry.getHeader().getLogfileOffset();
                        long executeTime = entry.getHeader().getExecuteTime();

                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);
                        break;
                    }
                }

                if (entryCount.get() > 0) {
                    controller.stop();
                    timeoutChecker.stop();
                    timeoutChecker.touch();
                }
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                System.out.println(logPosition);
            }

            public LogPosition getLatestIndexBy(String destination) {
                LogPosition masterLogPosition = new LogPosition();
                masterLogPosition.setIdentity(new LogIdentity(new InetSocketAddress("127.0.0.1", 3306), 1234L));
                masterLogPosition.setPostion(new EntryPosition(1322803601000L));
                return masterLogPosition;
            }
        });

        controller.start();
        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        Assert.assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        Assert.assertEquals(entryPosition.getJournalName(), "mysql-bin.000001");
        Assert.assertTrue(entryPosition.getPosition() <= 6163L);
        Assert.assertTrue(entryPosition.getTimestamp() <= defaultPosition.getTimestamp());
    }

    @Test
    public void test_no_position() throws InterruptedException { // 在某个文件下，找不到对应的timestamp数据，会使用106L
                                                                 // position进行数据抓取
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3 * 60 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition defaultPosition = buildPosition("mysql-bin.000001",
            null,
            new Date().getTime() + 1000 * 1000L);
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    if (entry.getEntryType() != EntryType.HEARTBEAT) {
                        entryCount.incrementAndGet();

                        // String logfilename =
                        // entry.getHeader().getLogfileName();
                        // long logfileoffset =
                        // entry.getHeader().getLogfileOffset();
                        long executeTime = entry.getHeader().getExecuteTime();

                        // entryPosition.setJournalName(logfilename);
                        // entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);
                        break;
                    }
                }

                if (entryCount.get() > 0) {
                    controller.stop();
                    timeoutChecker.stop();
                    timeoutChecker.touch();
                }
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractLogPositionManager() {

            public void persistLogPosition(String destination, LogPosition logPosition) {
                System.out.println(logPosition);
            }

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }
        });

        controller.start();
        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        Assert.assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        // Assert.assertEquals(logfilename, "mysql-bin.000001");
        // Assert.assertEquals(106L, logfileoffset);
        Assert.assertTrue(entryPosition.getTimestamp() < defaultPosition.getTimestamp());
    }

    // ======================== helper method =======================

    private EntryPosition buildPosition(String binlogFile, Long offest, Long timestamp) {
        return new EntryPosition(binlogFile, offest, timestamp);
    }

    private AuthenticationInfo buildAuthentication() {
        return new AuthenticationInfo(new InetSocketAddress(MYSQL_ADDRESS, 3306), USERNAME, PASSWORD);
    }
}

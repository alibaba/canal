package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
@Ignore
public class LocalBinlogEventParserTest {

    private static final String MYSQL_ADDRESS = "127.0.0.1";
    private static final String USERNAME      = "canal";
    private static final String PASSWORD      = "canal";
    private String              directory;

    @Before
    public void setUp() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        directory = new File(dummyFile + "/binlog/tsdb").getPath();
    }

    @Test
    public void test_position() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker();
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final EntryPosition defaultPosition = buildPosition("mysql-bin.000003", 219L, 1505467103000L);
        final LocalBinlogEventParser controller = new LocalBinlogEventParser();
        controller.setMasterPosition(defaultPosition);
        controller.setMasterInfo(buildAuthentication());
        controller.setDirectory(directory);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                entryCount.incrementAndGet();

                for (Entry entry : entrys) {
                    String logfilename = entry.getHeader().getLogfileName();
                    long logfileoffset = entry.getHeader().getLogfileOffset();
                    long executeTime = entry.getHeader().getExecuteTime();

                    entryPosition.setJournalName(logfilename);
                    entryPosition.setPosition(logfileoffset);
                    entryPosition.setTimestamp(executeTime);
                    break;
                }

                controller.stop();
                timeoutChecker.stop();
                timeoutChecker.touch();
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
        Assert.assertEquals(entryPosition, defaultPosition);
    }

    @Test
    public void test_timestamp() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(300 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final EntryPosition defaultPosition = buildPosition("mysql-bin.000003", null, 1505467103000L);
        final LocalBinlogEventParser controller = new LocalBinlogEventParser();
        controller.setMasterPosition(defaultPosition);
        controller.setMasterInfo(buildAuthentication());
        controller.setDirectory(directory);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    entryCount.incrementAndGet();

                    String logfilename = entry.getHeader().getLogfileName();
                    long logfileoffset = entry.getHeader().getLogfileOffset();
                    long executeTime = entry.getHeader().getExecuteTime();

                    entryPosition.setJournalName(logfilename);
                    entryPosition.setPosition(logfileoffset);
                    entryPosition.setTimestamp(executeTime);
                    break;
                }

                controller.stop();
                timeoutChecker.stop();
                timeoutChecker.touch();
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
        Assert.assertEquals(entryPosition.getJournalName(), "mysql-bin.000003");
        Assert.assertTrue(entryPosition.getPosition() <= 300L);
        Assert.assertTrue(entryPosition.getTimestamp() <= defaultPosition.getTimestamp());
    }

    @Test
    public void test_no_position() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3 * 1000);
        final EntryPosition defaultPosition = buildPosition("mysql-bin.000003",
            null,
            new Date().getTime() + 1000 * 1000L);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final LocalBinlogEventParser controller = new LocalBinlogEventParser();
        controller.setMasterPosition(defaultPosition);
        controller.setMasterInfo(buildAuthentication());
        controller.setDirectory(directory);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                for (Entry entry : entrys) {
                    entryCount.incrementAndGet();

                    String logfilename = entry.getHeader().getLogfileName();
                    long logfileoffset = entry.getHeader().getLogfileOffset();
                    long executeTime = entry.getHeader().getExecuteTime();

                    entryPosition.setJournalName(logfilename);
                    entryPosition.setPosition(logfileoffset);
                    entryPosition.setTimestamp(executeTime);
                    break;
                }

                controller.stop();
                timeoutChecker.stop();
                timeoutChecker.touch();
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
        // assertEquals(entryPosition.getJournalName(), "mysql-bin.000002");
        Assert.assertTrue(entryPosition.getTimestamp() <= defaultPosition.getTimestamp());
    }

    private EntryPosition buildPosition(String binlogFile, Long offest, Long timestamp) {
        return new EntryPosition(binlogFile, offest, timestamp);
    }

    private AuthenticationInfo buildAuthentication() {
        return new AuthenticationInfo(new InetSocketAddress(MYSQL_ADDRESS, 3306), USERNAME, PASSWORD);
    }
}

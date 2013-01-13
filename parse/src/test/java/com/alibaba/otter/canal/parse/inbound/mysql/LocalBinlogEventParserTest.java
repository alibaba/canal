package com.alibaba.otter.canal.parse.inbound.mysql;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.erosa.parse.DefaultMysqlBinlogParser;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry.Entry;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry.EntryType;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.stub.AbstractCanalLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;

public class LocalBinlogEventParserTest {

    private static final String MYSQL_ADDRESS = "10.20.153.51";
    private static final String USERNAME      = "retl";
    private static final String PASSWORD      = "retl";
    private String              directory;

    @Before
    public void setUp() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        directory = new File(dummyFile.getParent() + "/binlog").getPath();
        // directory = "/home/jianghang/work/otter-4.1.0/canal/parse/src/test/resources/binlog";
    }

    @Test
    public void test_position() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker();
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final EntryPosition defaultPosition = buildPosition("mysql-bin.000001", 6163L, 1322803601000L);
        final LocalBinlogEventParser controller = new LocalBinlogEventParser();
        controller.setMasterPosition(defaultPosition);
        controller.setDirectory(directory);
        controller.setBinlogParser(buildParser(buildAuthentication()));
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                entryCount.incrementAndGet();

                for (Entry entry : entrys) {
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

        controller.start();

        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        assertEquals(entryPosition, defaultPosition);
    }

    @Test
    public void test_timestamp() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(300 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final EntryPosition defaultPosition = buildPosition("mysql-bin.000001", null, 1322803601000L);
        final LocalBinlogEventParser controller = new LocalBinlogEventParser();
        controller.setMasterPosition(defaultPosition);
        controller.setDirectory(directory);
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
    public void test_no_position() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3 * 1000);
        final AtomicBoolean sinkExecuted = new AtomicBoolean(false);

        final EntryPosition defaultPosition = buildPosition("mysql-bin.000001", null,
                                                            new Date().getTime() + 1000 * 1000L);
        final LocalBinlogEventParser controller = new LocalBinlogEventParser();
        controller.setMasterPosition(defaultPosition);
        controller.setDirectory(directory);
        controller.setBinlogParser(buildParser(buildAuthentication()));
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            @Override
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException {
                sinkExecuted.set(true);
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

        controller.start();

        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        assertFalse("can't not execute this code", sinkExecuted.get());
    }

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

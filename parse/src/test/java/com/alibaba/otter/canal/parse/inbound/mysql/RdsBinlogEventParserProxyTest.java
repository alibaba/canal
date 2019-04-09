package com.alibaba.otter.canal.parse.inbound.mysql;

import java.net.InetSocketAddress;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogEventParserProxy;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;

/**
 * @author chengjin.lyf on 2018/7/21 下午5:24
 * @since 1.0.25
 */
@Ignore
public class RdsBinlogEventParserProxyTest {

    private static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    private static final String MYSQL_ADDRESS = "";
    private static final String USERNAME      = "";
    private static final String PASSWORD      = "";
    public static final String  DBNAME        = "";
    public static final String  TBNAME        = "";
    public static final String  DDL           = "";

    @Test
    public void test_timestamp() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3000 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final RdsBinlogEventParserProxy controller = new RdsBinlogEventParserProxy();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR_OF_DAY, -24 * 4);
        final EntryPosition defaultPosition = buildPosition(null, null, calendar.getTimeInMillis());
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setDetectingSQL(DETECTING_SQL);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setInstanceId("");
        controller.setAccesskey("");
        controller.setSecretkey("");
        controller.setDirectory("/tmp/binlog");
        controller.setEventBlackFilter(new AviaterRegexFilter("mysql\\.*"));
        controller.setFilterTableError(true);
        controller.setBatchFileSize(4);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<CanalEntry.Entry>>() {

            @Override
            public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                                   throws CanalSinkException {
                for (CanalEntry.Entry entry : entrys) {
                    if (entry.getEntryType() != CanalEntry.EntryType.HEARTBEAT) {
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
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractLogPositionManager() {

            private LogPosition logPosition;

            public void persistLogPosition(String destination, LogPosition logPosition) {
                this.logPosition = logPosition;
            }

            public LogPosition getLatestIndexBy(String destination) {
                return logPosition;
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

    // ======================== helper method =======================

    private EntryPosition buildPosition(String binlogFile, Long offest, Long timestamp) {
        return new EntryPosition(binlogFile, offest, timestamp);
    }

    private AuthenticationInfo buildAuthentication() {
        return new AuthenticationInfo(new InetSocketAddress(MYSQL_ADDRESS, 3306), USERNAME, PASSWORD);
    }
}

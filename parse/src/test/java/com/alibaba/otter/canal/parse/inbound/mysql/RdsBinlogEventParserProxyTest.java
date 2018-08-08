package com.alibaba.otter.canal.parse.inbound.mysql;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogEventParserProxy;
import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.TableMetaEntry;
import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.impl.mysql.MySqlTableMetaCallback;
import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.impl.mysql.MySqlTableMetaStorageFactory;
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
public class RdsBinlogEventParserProxyTest {

    private static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    private static final String MYSQL_ADDRESS = "";
    private static final String USERNAME      = "";
    private static final String PASSWORD      = "";
    public static final String DBNAME = "";
    public static final String TBNAME = "";
    public static final String DDL = "";


    @Test
    public void test_timestamp() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3000 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final RdsBinlogEventParserProxy controller = new RdsBinlogEventParserProxy();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        final EntryPosition defaultPosition = buildPosition(null, null, calendar.getTimeInMillis());
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setDetectingSQL(DETECTING_SQL);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setInstanceId("");
        controller.setAccesskey("");
        controller.setSecretkey("");
        controller.setBatchSize(4);
//        controller.setRdsOpenApiUrl("https://rds.aliyuncs.com/");
        controller.setTableMetaStorageFactory(new MySqlTableMetaStorageFactory(new MySqlTableMetaCallback() {
            @Override
            public void save(String dbAddress, String schema, String table, String ddl, Long timestamp) {

            }

            @Override
            public List<TableMetaEntry> fetch(String dbAddress, String dbName) {
                TableMetaEntry tableMeta = new TableMetaEntry();
                tableMeta.setSchema(DBNAME);
                tableMeta.setTable(TBNAME);
                tableMeta.setDdl(DDL);
                tableMeta.setTimestamp(new Date().getTime());
                List<TableMetaEntry> entries = new ArrayList<TableMetaEntry>();
                entries.add(tableMeta);
                return entries;
            }

            @Override
            public List<TableMetaEntry> fetch(String dbAddress, String dbName, String tableName) {
                TableMetaEntry tableMeta = new TableMetaEntry();
                tableMeta.setSchema(DBNAME);
                tableMeta.setTable(TBNAME);
                tableMeta.setDdl(DDL);
                tableMeta.setTimestamp(new Date().getTime());
                List<TableMetaEntry> entries = new ArrayList<TableMetaEntry>();
                entries.add(tableMeta);
                return entries;
            }
        }, DBNAME));
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
                System.out.println(logPosition);
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

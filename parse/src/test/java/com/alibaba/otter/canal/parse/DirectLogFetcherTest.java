package com.alibaba.otter.canal.parse;

import static com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher.MASTER_HEARTBEAT_PERIOD_SECONDS;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlQueryExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.RegisterSlaveCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer;
import com.taobao.tddl.dbsync.binlog.event.RowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsQueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent.ColumnInfo;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.XidLogEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.AnnotateRowsEvent;

@Ignore
public class DirectLogFetcherTest {

    protected final Logger logger         = LoggerFactory.getLogger(this.getClass());
    protected String       binlogFileName = "mysql-bin.000001";
    protected Charset      charset        = Charset.forName("utf-8");
    private int            binlogChecksum;

    @Test
    public void testSimple() {
        DirectLogFetcher fetcher = new DirectLogFetcher();
        try {
            MysqlConnector connector = new MysqlConnector(new InetSocketAddress("127.0.0.1", 3306), "root", "hello");
            connector.connect();
            updateSettings(connector);
            loadBinlogChecksum(connector);
            sendRegisterSlave(connector, 3);
            sendBinlogDump(connector, "mysql-bin.000001", 4L, 3);

            fetcher.start(connector.getChannel());

            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));

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
                        System.out.println(((RotateLogEvent) event).getFilename());
                        break;
                    case LogEvent.TABLE_MAP_EVENT:
                        parseTableMapEvent((TableMapLogEvent) event);
                        break;
                    case LogEvent.WRITE_ROWS_EVENT_V1:
                    case LogEvent.WRITE_ROWS_EVENT:
                        parseRowsEvent((WriteRowsLogEvent) event);
                        break;
                    case LogEvent.UPDATE_ROWS_EVENT_V1:
                    case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
                    case LogEvent.UPDATE_ROWS_EVENT:
                        parseRowsEvent((UpdateRowsLogEvent) event);
                        break;
                    case LogEvent.DELETE_ROWS_EVENT_V1:
                    case LogEvent.DELETE_ROWS_EVENT:
                        parseRowsEvent((DeleteRowsLogEvent) event);
                        break;
                    case LogEvent.QUERY_EVENT:
                        parseQueryEvent((QueryLogEvent) event);
                        break;
                    case LogEvent.ROWS_QUERY_LOG_EVENT:
                        parseRowsQueryEvent((RowsQueryLogEvent) event);
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

    private void sendRegisterSlave(MysqlConnector connector, int slaveId) throws IOException {
        RegisterSlaveCommandPacket cmd = new RegisterSlaveCommandPacket();
        cmd.reportHost = connector.getAddress().getAddress().getHostAddress();
        cmd.reportPasswd = connector.getPassword();
        cmd.reportUser = connector.getUsername();
        cmd.serverId = slaveId;
        byte[] cmdBody = cmd.toBytes();

        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(cmdBody.length);
        header.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), header.toBytes(), cmdBody);

        header = PacketManager.readHeader(connector.getChannel(), 4);
        byte[] body = PacketManager.readBytes(connector.getChannel(), header.getPacketBodyLength());
        assert body != null;
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Register slave:" + err.toString());
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
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
        PacketManager.writePkg(connector.getChannel(), binlogDumpHeader.toBytes(), cmdBody);
    }

    private void updateSettings(MysqlConnector connector) throws IOException {
        try {
            update("set wait_timeout=9999999", connector);
        } catch (Exception e) {
            logger.warn("update wait_timeout failed", e);
        }
        try {
            update("set net_write_timeout=7200", connector);
        } catch (Exception e) {
            logger.warn("update net_write_timeout failed", e);
        }

        try {
            update("set net_read_timeout=7200", connector);
        } catch (Exception e) {
            logger.warn("update net_read_timeout failed", e);
        }

        try {
            // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
            update("set names 'binary'", connector);
        } catch (Exception e) {
            logger.warn("update names failed", e);
        }

        try {
            // mysql5.6针对checksum支持需要设置session变量
            // 如果不设置会出现错误： Slave can not handle replication events with the
            // checksum that master is configured to log
            // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
            update("set @master_binlog_checksum= @@global.binlog_checksum", connector);
        } catch (Exception e) {
            logger.warn("update master_binlog_checksum failed", e);
        }

        try {
            // 参考:https://github.com/alibaba/canal/issues/284
            // mysql5.6需要设置slave_uuid避免被server kill链接
            update("set @slave_uuid=uuid()", connector);
        } catch (Exception e) {
            if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
                logger.warn("update slave_uuid failed", e);
            }
        }

        try {
            // mariadb针对特殊的类型，需要设置session变量
            update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'", connector);
        } catch (Exception e) {
            logger.warn("update mariadb_slave_capability failed", e);
        }

        try {
            long period = TimeUnit.SECONDS.toNanos(MASTER_HEARTBEAT_PERIOD_SECONDS);
            update("SET @master_heartbeat_period=" + period, connector);
        } catch (Exception e) {
            logger.warn("update master_heartbeat_period failed", e);
        }
    }

    private void loadBinlogChecksum(MysqlConnector connector) {
        ResultSetPacket rs = null;
        try {
            rs = query("select @@global.binlog_checksum", connector);
        } catch (IOException e) {
            throw new CanalParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues != null && columnValues.size() >= 1 && columnValues.get(0).toUpperCase().equals("CRC32")) {
            binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_CRC32;
        } else {
            binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_OFF;
        }
    }

    public ResultSetPacket query(String cmd, MysqlConnector connector) throws IOException {
        MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
        return exector.query(cmd);
    }

    public void update(String cmd, MysqlConnector connector) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    protected void parseQueryEvent(QueryLogEvent event) {
        System.out.println(String.format("================> binlog[%s:%s] , name[%s]",
            binlogFileName,
            event.getHeader().getLogPos() - event.getHeader().getEventLen(),
            event.getCatalog()));
        System.out.println("sql : " + event.getQuery());
    }

    protected void parseRowsQueryEvent(RowsQueryLogEvent event) throws Exception {
        System.out.println(String.format("================> binlog[%s:%s]", binlogFileName, event.getHeader()
            .getLogPos() - event.getHeader().getEventLen()));
        System.out.println("sql : " + new String(event.getRowsQuery().getBytes("ISO-8859-1"), charset.name()));
    }

    protected void parseAnnotateRowsEvent(AnnotateRowsEvent event) throws Exception {
        System.out.println(String.format("================> binlog[%s:%s]", binlogFileName, event.getHeader()
            .getLogPos() - event.getHeader().getEventLen()));
        System.out.println("sql : " + new String(event.getRowsQuery().getBytes("ISO-8859-1"), charset.name()));
    }

    public void parseTableMapEvent(TableMapLogEvent event) {
        try {
            String charsetDbName = new String(event.getDbName().getBytes("ISO-8859-1"), charset.name());
            event.setDbname(charsetDbName);

            String charsetTbName = new String(event.getTableName().getBytes("ISO-8859-1"), charset.name());
            event.setTblname(charsetTbName);
        } catch (UnsupportedEncodingException e) {
            throw new CanalParseException(e);
        }
    }

    protected void parseXidEvent(XidLogEvent event) throws Exception {
        System.out.println(String.format("================> binlog[%s:%s]", binlogFileName, event.getHeader()
            .getLogPos() - event.getHeader().getEventLen()));
        System.out.println("xid : " + event.getXid());
    }

    protected void parseRowsEvent(RowsLogEvent event) {
        try {
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s]",
                binlogFileName,
                event.getHeader().getLogPos() - event.getHeader().getEventLen(),
                event.getTable().getDbName(),
                event.getTable().getTableName()));
            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getChangeColumns();
            while (buffer.nextOneRow(columns)) {
                // 处理row记录
                int type = event.getHeader().getType();
                if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
                    // insert的记录放在before字段中
                    parseOneRow(event, buffer, columns, true);
                } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
                    // delete的记录放在before字段中
                    parseOneRow(event, buffer, columns, false);
                } else {
                    // update需要处理before/after
                    System.out.println("-------> before");
                    parseOneRow(event, buffer, columns, false);
                    if (!buffer.nextOneRow(changeColumns, true)) {
                        break;
                    }
                    System.out.println("-------> after");
                    parseOneRow(event, buffer, changeColumns, true);
                }

            }
        } catch (Exception e) {
            throw new RuntimeException("parse row data failed.", e);
        }
    }

    protected void parseOneRow(RowsLogEvent event, RowsLogBuffer buffer, BitSet cols, boolean isAfter)
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
                //
            } else {
                final Serializable value = buffer.getValue();
                if (value instanceof byte[]) {
                    System.out.println(new String((byte[]) value));
                } else {
                    System.out.println(value);
                }
            }
        }

    }

}

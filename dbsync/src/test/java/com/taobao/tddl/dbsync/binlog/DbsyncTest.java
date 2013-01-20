package com.taobao.tddl.dbsync.binlog;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.BitSet;

import org.junit.Test;

import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer;
import com.taobao.tddl.dbsync.binlog.event.RowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent.ColumnInfo;

public class DbsyncTest {

    private String  binlogFileName = "mysql-bin.000001";
    private Charset charset        = Charset.forName("gbK");

    @Test
    public void testSimple() {
        try {
            DirectLogFetcher fecther = new DirectLogFetcher();
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://10.20.153.51:3306", "retl", "retl");
            fecther.open(connection, "mysql-bin.000002", 38266L, 1);

            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            while (fecther.fetch()) {
                LogEvent event = null;
                event = decoder.decode(fecther, context);

                if (event == null) {
                    throw new RuntimeException("parse failed");
                }

                int eventType = event.getHeader().getType();
                switch (eventType) {
                    case LogEvent.ROTATE_EVENT:
                        binlogFileName = ((RotateLogEvent) event).getFilename();
                        break;
                    case LogEvent.WRITE_ROWS_EVENT:
                        parseRowsEvent((WriteRowsLogEvent) event);
                        break;
                    case LogEvent.UPDATE_ROWS_EVENT:
                        parseRowsEvent((UpdateRowsLogEvent) event);
                        break;
                    case LogEvent.DELETE_ROWS_EVENT:
                        parseRowsEvent((DeleteRowsLogEvent) event);
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void parseRowsEvent(RowsLogEvent event) {
        try {
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s]", binlogFileName,
                                             event.getHeader().getLogPos() - event.getHeader().getEventLen(),
                                             event.getTable().getDbName(), event.getTable().getTableName()));
            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getChangeColumns();
            while (buffer.nextOneRow(columns)) {
                // 处理row记录
                if (LogEvent.WRITE_ROWS_EVENT == event.getHeader().getType()) {
                    // insert的记录放在before字段中
                    parseOneRow(event, buffer, columns, true);
                } else if (LogEvent.DELETE_ROWS_EVENT == event.getHeader().getType()) {
                    // delete的记录放在before字段中
                    parseOneRow(event, buffer, columns, false);
                } else {
                    // update需要处理before/after
                    parseOneRow(event, buffer, columns, false);
                    parseOneRow(event, buffer, changeColumns, true);
                }

            }
        } catch (Exception e) {
            throw new RuntimeException("parse row data failed.", e);
        }
    }

    private void parseOneRow(RowsLogEvent event, RowsLogBuffer buffer, BitSet cols, boolean isAfter)
                                                                                                    throws UnsupportedEncodingException {
        TableMapLogEvent map = event.getTable();
        if (map == null) {
            throw new RuntimeException("not found TableMap with tid=" + event.getTableId());
        }

        final int columnCnt = map.getColumnCnt();
        final ColumnInfo[] columnInfo = map.getColumnInfo();

        for (int i = 0; i < columnCnt; i++) {
            ColumnInfo info = columnInfo[i];
            buffer.nextValue(info.type, info.meta);

            if (buffer.isNull()) {
                //
            } else {
                final Serializable value = buffer.getValue();
                System.out.println(value);
            }
        }

    }
}

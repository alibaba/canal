package com.taobao.tddl.dbsync.binlog;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.Assert;

import org.junit.Test;

import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;

public class DirectLogFetcherTest extends BaseLogFetcherTest {

    @Test
    public void testSimple() {
        DirectLogFetcher fecther = new DirectLogFetcher();
        try {
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
                    case LogEvent.QUERY_EVENT:
                        parseQueryEvent((QueryLogEvent) event);
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            try {
                fecther.close();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }
    }
}

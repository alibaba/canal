package com.taobao.tddl.dbsync.binlog;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsQueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.XidLogEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.AnnotateRowsEvent;
@Ignore
public class DirectLogFetcherTest extends BaseLogFetcherTest {

    @Test
    public void testSimple() {
        DirectLogFetcher fecther = new DirectLogFetcher();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306", "root", "hello");
            Statement statement = connection.createStatement();
            statement.execute("SET @master_binlog_checksum='@@global.binlog_checksum'");
            statement.execute("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");

            fecther.open(connection, "mysql-bin.000007", 89797036L, 2);

            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            while (fecther.fetch()) {
                LogEvent event = decoder.decode(fecther, context);
                int eventType = event.getHeader().getType();
                switch (eventType) {
                    case LogEvent.ROTATE_EVENT:
                        binlogFileName = ((RotateLogEvent) event).getFilename();
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
                        parseAnnotateRowsEvent((AnnotateRowsEvent) event);
                        break;
                    case LogEvent.XID_EVENT:
                        parseXidEvent((XidLogEvent) event);
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
                fecther.close();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }

    }
}

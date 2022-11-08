package com.taobao.tddl.dbsync.binlog;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.junit.Assert;
import org.junit.Before;
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
public class FileLogFetcherTest extends BaseLogFetcherTest {

    private String directory;

    @Before
    public void setUp() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        directory = new File(dummyFile.getParent() + "/binlog").getPath();
    }

    @Test
    public void testSimple() {
        FileLogFetcher fetcher = new FileLogFetcher(1024 * 16);
        try {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();

            File current = new File(directory, "mysql-bin.000001");
            fetcher.open(current, 2051L);
            context.setLogPosition(new LogPosition(current.getName()));

            while (fetcher.fetch()) {
                LogEvent event = null;
                event = decoder.decode(fetcher, context);
                if (event != null) {
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
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            try {
                fetcher.close();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }
    }
}

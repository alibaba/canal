package com.alibaba.otter.canal.parse.inbound.mysql;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsLocalBinlogEventParser;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;

/**
 * @author agapple 2017年10月15日 下午2:16:58
 * @since 1.0.25
 */
@Ignore
public class RdsLocalBinlogDumpTest {

    @Test
    public void testSimple() {
        String directory = "/tmp/rds";
        final RdsLocalBinlogEventParser controller = new RdsLocalBinlogEventParser();
        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress("127.0.0.1", 3306), "root", "hello"));
        controller.setConnectionCharsetStd(Charset.forName("UTF-8"));
        controller.setDirectory(directory);
        controller.setAccesskey("");
        controller.setSecretkey("");
        controller.setInstanceId("");
        controller.setStartTime(1507860498350L);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException,
                                                                                                        InterruptedException {

                for (Entry entry : entrys) {
                    if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                        || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                        continue;
                    }

                    if (entry.getEntryType() == EntryType.ROWDATA) {
                        RowChange rowChage = null;
                        try {
                            rowChage = RowChange.parseFrom(entry.getStoreValue());
                        } catch (Exception e) {
                            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:"
                                                       + entry.toString(), e);
                        }

                        EventType eventType = rowChage.getEventType();
                        System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getSchemaName(),
                            entry.getHeader().getTableName(),
                            eventType));

                        for (RowData rowData : rowChage.getRowDatasList()) {
                            if (eventType == EventType.DELETE) {
                                print(rowData.getBeforeColumnsList());
                            } else if (eventType == EventType.INSERT) {
                                print(rowData.getAfterColumnsList());
                            } else {
                                System.out.println("-------> before");
                                print(rowData.getBeforeColumnsList());
                                System.out.println("-------> after");
                                print(rowData.getAfterColumnsList());
                            }
                        }
                    }
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

        try {
            Thread.sleep(100 * 1000 * 1000L);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
        controller.stop();
    }

    private void print(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}

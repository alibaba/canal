package com.alibaba.otter.canal.client.adapter.support;

import java.util.*;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Message对象解析工具类
 *
 * @author machengyuan 2018-8-19 下午06:14:23
 * @version 1.0.0
 */
public class MessageUtil {

    public static void parse4Dml(Message message, Consumer<Dml> consumer) {
        if (message == null) {
            return;
        }
        List<CanalEntry.Entry> entries = message.getEntries();

        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                    e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            final Dml dml = new Dml();
            dml.setDatabase(entry.getHeader().getSchemaName());
            dml.setTable(entry.getHeader().getTableName());
            dml.setType(eventType.toString());
            dml.setTs(System.currentTimeMillis());
            dml.setSql(rowChange.getSql());
            List<Map<String, Object>> data = new ArrayList<>();
            List<Map<String, Object>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                        && eventType != CanalEntry.EventType.DELETE) {
                        continue;
                    }

                    Map<String, Object> row = new LinkedHashMap<>();
                    List<CanalEntry.Column> columns;

                    if (eventType == CanalEntry.EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else {
                        columns = rowData.getAfterColumnsList();
                    }

                    for (CanalEntry.Column column : columns) {
                        row.put(column.getName(),
                            JdbcTypeUtil.typeConvert(dml.getTable(),
                                column.getName(),
                                column.getValue(),
                                column.getSqlType(),
                                column.getMysqlType()));
                        // 获取update为true的字段
                        if (column.getUpdated()) {
                            updateSet.add(column.getName());
                        }
                    }
                    if (!row.isEmpty()) {
                        data.add(row);
                    }

                    if (eventType == CanalEntry.EventType.UPDATE) {
                        Map<String, Object> rowOld = new LinkedHashMap<>();
                        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                            if (updateSet.contains(column.getName())) {
                                rowOld.put(column.getName(),
                                    JdbcTypeUtil.typeConvert(dml.getTable(),
                                        column.getName(),
                                        column.getValue(),
                                        column.getSqlType(),
                                        column.getMysqlType()));
                            }
                        }
                        // update操作将记录修改前的值
                        if (!rowOld.isEmpty()) {
                            old.add(rowOld);
                        }
                    }
                }
                if (!data.isEmpty()) {
                    dml.setData(data);
                }
                if (!old.isEmpty()) {
                    dml.setOld(old);
                }
                consumer.accept(dml);
            }
        }
    }

    public interface Consumer<T> {

        void accept(T t);
    }
}

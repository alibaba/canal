package com.alibaba.otter.canal.connector.core.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Message对象解析工具类
 *
 * @author rewerma 2018-8-19 下午06:14:23
 * @version 1.0.0
 */
public class MessageUtil {

    public static List<CommonMessage> convert(Message message) {
        if (message == null) {
            return null;
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        List<CommonMessage> msgs = new ArrayList<>(entries.size());
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

            final CommonMessage msg = new CommonMessage();
            msg.setIsDdl(rowChange.getIsDdl());
            msg.setDatabase(entry.getHeader().getSchemaName());
            msg.setTable(entry.getHeader().getTableName());
            msg.setType(eventType.toString());
            msg.setEs(entry.getHeader().getExecuteTime());
            msg.setIsDdl(rowChange.getIsDdl());
            msg.setTs(System.currentTimeMillis());
            msg.setSql(rowChange.getSql());
            msgs.add(msg);
            List<Map<String, Object>> data = new ArrayList<>();
            List<Map<String, Object>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                msg.setPkNames(new ArrayList<>());
                int i = 0;
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
                        if (i == 0) {
                            if (column.getIsKey()) {
                                msg.getPkNames().add(column.getName());
                            }
                        }
                        if (column.getIsNull()) {
                            row.put(column.getName(), null);
                        } else {
                            row.put(column.getName(),
                                JdbcTypeUtil.typeConvert(msg.getTable(),
                                    column.getName(),
                                    column.getValue(),
                                    column.getSqlType(),
                                    column.getMysqlType()));
                        }
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
                                if (column.getIsNull()) {
                                    rowOld.put(column.getName(), null);
                                } else {
                                    rowOld.put(column.getName(), JdbcTypeUtil.typeConvert(msg.getTable(),
                                        column.getName(),
                                        column.getValue(),
                                        column.getSqlType(),
                                        column.getMysqlType()));
                                }
                            }
                        }
                        // update操作将记录修改前的值
                        if (!rowOld.isEmpty()) {
                            old.add(rowOld);
                        }
                    }

                    i++;
                }
                if (!data.isEmpty()) {
                    msg.setData(data);
                }
                if (!old.isEmpty()) {
                    msg.setOld(old);
                }
            }
        }

        return msgs;
    }
}

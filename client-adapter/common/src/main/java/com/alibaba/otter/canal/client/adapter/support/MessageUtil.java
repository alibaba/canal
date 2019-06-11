package com.alibaba.otter.canal.client.adapter.support;

import java.util.*;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

/**
 * Message对象解析工具类
 *
 * @author rewerma 2018-8-19 下午06:14:23
 * @version 1.0.0
 */
public class MessageUtil {

    public static List<Dml> parse4Dml(String destination, String groupId, Message message) {
        if (message == null) {
            return null;
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        List<Dml> dmls = new ArrayList<Dml>(entries.size());
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
            dml.setIsDdl(rowChange.getIsDdl());
            dml.setDestination(destination);
            dml.setGroupId(groupId);
            dml.setDatabase(entry.getHeader().getSchemaName());
            dml.setTable(entry.getHeader().getTableName());
            dml.setType(eventType.toString());
            dml.setEs(entry.getHeader().getExecuteTime());
            dml.setIsDdl(rowChange.getIsDdl());
            dml.setTs(System.currentTimeMillis());
            dml.setSql(rowChange.getSql());
            dmls.add(dml);
            List<Map<String, Object>> data = new ArrayList<>();
            List<Map<String, Object>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                dml.setPkNames(new ArrayList<>());
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
                                dml.getPkNames().add(column.getName());
                            }
                        }
                        if (column.getIsNull()) {
                            row.put(column.getName(), null);
                        } else {
                            row.put(column.getName(),
                                JdbcTypeUtil.typeConvert(dml.getTable(),
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
                                    rowOld.put(column.getName(),
                                        JdbcTypeUtil.typeConvert(dml.getTable(),
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
                    dml.setData(data);
                }
                if (!old.isEmpty()) {
                    dml.setOld(old);
                }
            }
        }

        return dmls;
    }

    public static List<Dml> flatMessage2Dml(String destination, String groupId, List<FlatMessage> flatMessages) {
        List<Dml> dmls = new ArrayList<Dml>(flatMessages.size());
        for (FlatMessage flatMessage : flatMessages) {
            Dml dml = flatMessage2Dml(destination, groupId, flatMessage);
            if (dml != null) {
                dmls.add(dml);
            }
        }

        return dmls;
    }

    public static Dml flatMessage2Dml(String destination, String groupId, FlatMessage flatMessage) {
        if (flatMessage == null) {
            return null;
        }
        Dml dml = new Dml();
        dml.setDestination(destination);
        dml.setGroupId(groupId);
        dml.setDatabase(flatMessage.getDatabase());
        dml.setTable(flatMessage.getTable());
        dml.setPkNames(flatMessage.getPkNames());
        dml.setIsDdl(flatMessage.getIsDdl());
        dml.setType(flatMessage.getType());
        dml.setTs(flatMessage.getTs());
        dml.setEs(flatMessage.getEs());
        dml.setSql(flatMessage.getSql());
        // if (flatMessage.getSqlType() == null || flatMessage.getMysqlType() == null) {
        // throw new RuntimeException("SqlType or mysqlType is null");
        // }
        List<Map<String, String>> data = flatMessage.getData();
        if (data != null) {
            dml.setData(changeRows(dml.getTable(), data, flatMessage.getSqlType(), flatMessage.getMysqlType()));
        }
        List<Map<String, String>> old = flatMessage.getOld();
        if (old != null) {
            dml.setOld(changeRows(dml.getTable(), old, flatMessage.getSqlType(), flatMessage.getMysqlType()));
        }
        return dml;
    }

    private static List<Map<String, Object>> changeRows(String table, List<Map<String, String>> rows,
                                                        Map<String, Integer> sqlTypes, Map<String, String> mysqlTypes) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, String> row : rows) {
            Map<String, Object> resultRow = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : row.entrySet()) {
                String columnName = entry.getKey();
                String columnValue = entry.getValue();

                Integer sqlType = sqlTypes.get(columnName);
                if (sqlType == null) {
                    continue;
                }

                String mysqlType = mysqlTypes.get(columnName);
                if (mysqlType == null) {
                    continue;
                }

                Object finalValue = JdbcTypeUtil.typeConvert(table, columnName, columnValue, sqlType, mysqlType);
                resultRow.put(columnName, finalValue);
            }
            result.add(resultRow);
        }
        return result;
    }
}

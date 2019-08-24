package com.alibaba.otter.canal.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * process MQ Message utils
 *
 * @author agapple 2018年12月11日 下午1:28:32
 */
public class MQMessageUtils {

    @SuppressWarnings("deprecation")
    private static Map<String, List<PartitionData>>    partitionDatas    = MigrateMap.makeComputingMap(new MapMaker().softValues(),
                                                                             new Function<String, List<PartitionData>>() {

                                                                                 public List<PartitionData> apply(String pkHashConfigs) {
                                                                                     List<PartitionData> datas = Lists.newArrayList();

                                                                                     String[] pkHashConfigArray = StringUtils.split(StringUtils.replace(pkHashConfigs,
                                                                                         ",",
                                                                                         ";"),
                                                                                         ";");
                                                                                     // schema.table:id^name
                                                                                     for (String pkHashConfig : pkHashConfigArray) {
                                                                                         PartitionData data = new PartitionData();
                                                                                         int i = pkHashConfig.lastIndexOf(":");
                                                                                         if (i > 0) {
                                                                                             String pkStr = pkHashConfig.substring(i + 1);
                                                                                             if (pkStr.equalsIgnoreCase("$pk$")) {
                                                                                                 data.hashMode.autoPkHash = true;
                                                                                             } else {
                                                                                                 data.hashMode.pkNames = Lists.newArrayList(StringUtils.split(pkStr,
                                                                                                     '^'));
                                                                                             }

                                                                                             pkHashConfig = pkHashConfig.substring(0,
                                                                                                 i);
                                                                                         } else {
                                                                                             data.hashMode.tableHash = true;
                                                                                         }

                                                                                         if (!isWildCard(pkHashConfig)) {
                                                                                             data.simpleName = pkHashConfig;
                                                                                         } else {
                                                                                             data.regexFilter = new AviaterRegexFilter(pkHashConfig);
                                                                                         }
                                                                                         datas.add(data);
                                                                                     }

                                                                                     return datas;
                                                                                 }
                                                                             });

    @SuppressWarnings("deprecation")
    private static Map<String, List<DynamicTopicData>> dynamicTopicDatas = MigrateMap.makeComputingMap(new MapMaker().softValues(),
                                                                             new Function<String, List<DynamicTopicData>>() {

                                                                                 public List<DynamicTopicData> apply(String pkHashConfigs) {
                                                                                     List<DynamicTopicData> datas = Lists.newArrayList();
                                                                                     String[] dynamicTopicArray = StringUtils.split(StringUtils.replace(pkHashConfigs,
                                                                                         ",",
                                                                                         ";"),
                                                                                         ";");
                                                                                     // schema.table
                                                                                     for (String dynamicTopic : dynamicTopicArray) {
                                                                                         DynamicTopicData data = new DynamicTopicData();

                                                                                         if (!isWildCard(dynamicTopic)) {
                                                                                             data.simpleName = dynamicTopic;
                                                                                         } else {
                                                                                             if (dynamicTopic.contains("\\.")) {
                                                                                                 data.tableRegexFilter = new AviaterRegexFilter(dynamicTopic);
                                                                                             } else {
                                                                                                 data.schemaRegexFilter = new AviaterRegexFilter(dynamicTopic);
                                                                                             }
                                                                                         }
                                                                                         datas.add(data);
                                                                                     }

                                                                                     return datas;
                                                                                 }
                                                                             });

    /**
     * 按 schema 或者 schema+table 将 message 分配到对应topic
     *
     * @param message 原message
     * @param defaultTopic 默认topic
     * @param dynamicTopicConfigs 动态topic规则
     * @return 分隔后的message map
     */
    public static Map<String, Message> messageTopics(Message message, String defaultTopic, String dynamicTopicConfigs) {
        List<CanalEntry.Entry> entries;
        if (message.isRaw()) {
            List<ByteString> rawEntries = message.getRawEntries();
            entries = new ArrayList<>(rawEntries.size());
            for (ByteString byteString : rawEntries) {
                CanalEntry.Entry entry;
                try {
                    entry = CanalEntry.Entry.parseFrom(byteString);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                entries.add(entry);
            }
        } else {
            entries = message.getEntries();
        }
        Map<String, Message> messages = new HashMap<>();
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

            if (StringUtils.isEmpty(schemaName) || StringUtils.isEmpty(tableName)) {
                put2MapMessage(messages, message.getId(), defaultTopic, entry);
            } else {
                Set<String> topics = matchTopics(schemaName + "." + tableName, dynamicTopicConfigs);
                if (topics != null) {
                    for (String topic : topics) {
                        put2MapMessage(messages, message.getId(), topic, entry);
                    }
                } else {
                    topics = matchTopics(schemaName, dynamicTopicConfigs);
                    if (topics != null) {
                        for (String topic : topics) {
                            put2MapMessage(messages, message.getId(), topic, entry);
                        }
                    } else {
                        put2MapMessage(messages, message.getId(), defaultTopic, entry);
                    }
                }
            }
        }
        return messages;
    }

    /**
     * 将 message 分区
     *
     * @param partitionsNum 分区数
     * @param pkHashConfigs 分区库表主键正则表达式
     * @return 分区message数组
     */
    @SuppressWarnings("unchecked")
    public static Message[] messagePartition(Message message, Integer partitionsNum, String pkHashConfigs) {
        if (partitionsNum == null) {
            partitionsNum = 1;
        }
        Message[] partitionMessages = new Message[partitionsNum];
        List<Entry>[] partitionEntries = new List[partitionsNum];
        for (int i = 0; i < partitionsNum; i++) {
            partitionEntries[i] = new ArrayList<>();
        }

        List<CanalEntry.Entry> entries;
        if (message.isRaw()) {
            List<ByteString> rawEntries = message.getRawEntries();
            entries = new ArrayList<>(rawEntries.size());
            for (ByteString byteString : rawEntries) {
                Entry entry;
                try {
                    entry = Entry.parseFrom(byteString);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                entries.add(entry);
            }
        } else {
            entries = message.getEntries();
        }

        for (Entry entry : entries) {
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            if (rowChange.getIsDdl()) {
                partitionEntries[0].add(entry);
            } else {
                if (rowChange.getRowDatasList() != null && !rowChange.getRowDatasList().isEmpty()) {
                    String database = entry.getHeader().getSchemaName();
                    String table = entry.getHeader().getTableName();
                    HashMode hashMode = getPartitionHashColumns(database + "." + table, pkHashConfigs);
                    if (hashMode == null) {
                        // 如果都没有匹配，发送到第一个分区
                        partitionEntries[0].add(entry);
                    } else if (hashMode.tableHash) {
                        int hashCode = table.hashCode();
                        int pkHash = Math.abs(hashCode) % partitionsNum;
                        pkHash = Math.abs(pkHash);
                        // tableHash not need split entry message
                        partitionEntries[pkHash].add(entry);
                    } else {
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            int hashCode = database.hashCode();
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            List<CanalEntry.Column> columns = null;
                            if (eventType == CanalEntry.EventType.DELETE) {
                                columns = rowData.getBeforeColumnsList();
                            } else {
                                columns = rowData.getAfterColumnsList();
                            }

                            if (hashMode.autoPkHash) {
                                // isEmpty use default pkNames
                                for (CanalEntry.Column column : columns) {
                                    if (column.getIsKey()) {
                                        hashCode = hashCode ^ column.getValue().hashCode();
                                    }
                                }
                            } else {
                                for (CanalEntry.Column column : columns) {
                                    if (checkPkNamesHasContain(hashMode.pkNames, column.getName())) {
                                        hashCode = hashCode ^ column.getValue().hashCode();
                                    }
                                }
                            }

                            int pkHash = Math.abs(hashCode) % partitionsNum;
                            pkHash = Math.abs(pkHash);
                            // build new entry
                            Entry.Builder builder = Entry.newBuilder(entry);
                            RowChange.Builder rowChangeBuilder = RowChange.newBuilder(rowChange);
                            rowChangeBuilder.clearRowDatas();
                            rowChangeBuilder.addRowDatas(rowData);
                            builder.clearStoreValue();
                            builder.setStoreValue(rowChangeBuilder.build().toByteString());
                            partitionEntries[pkHash].add(builder.build());
                        }
                    }
                } else {
                    // 针对stmt/mixed binlog格式的query事件
                    partitionEntries[0].add(entry);
                }
            }
        }

        for (int i = 0; i < partitionsNum; i++) {
            List<Entry> entriesTmp = partitionEntries[i];
            if (!entriesTmp.isEmpty()) {
                partitionMessages[i] = new Message(message.getId(), entriesTmp);
            }
        }

        return partitionMessages;
    }

    /**
     * 将Message转换为FlatMessage
     *
     * @param message 原生message
     * @return FlatMessage列表
     */
    public static List<FlatMessage> messageConverter(Message message) {
        try {
            if (message == null) {
                return null;
            }

            List<FlatMessage> flatMessages = new ArrayList<>();
            List<CanalEntry.Entry> entrys = null;
            if (message.isRaw()) {
                List<ByteString> rawEntries = message.getRawEntries();
                entrys = new ArrayList<CanalEntry.Entry>(rawEntries.size());
                for (ByteString byteString : rawEntries) {
                    CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(byteString);
                    entrys.add(entry);
                }
            } else {
                entrys = message.getEntries();
            }

            for (CanalEntry.Entry entry : entrys) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    continue;
                }

                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:"
                                               + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChange.getEventType();

                FlatMessage flatMessage = new FlatMessage(message.getId());
                flatMessages.add(flatMessage);
                flatMessage.setDatabase(entry.getHeader().getSchemaName());
                flatMessage.setTable(entry.getHeader().getTableName());
                flatMessage.setIsDdl(rowChange.getIsDdl());
                flatMessage.setType(eventType.toString());
                flatMessage.setEs(entry.getHeader().getExecuteTime());
                flatMessage.setTs(System.currentTimeMillis());
                flatMessage.setSql(rowChange.getSql());

                if (!rowChange.getIsDdl()) {
                    Map<String, Integer> sqlType = new LinkedHashMap<>();
                    Map<String, String> mysqlType = new LinkedHashMap<>();
                    List<Map<String, String>> data = new ArrayList<>();
                    List<Map<String, String>> old = new ArrayList<>();

                    Set<String> updateSet = new HashSet<>();
                    boolean hasInitPkNames = false;
                    for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                        if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                            && eventType != CanalEntry.EventType.DELETE) {
                            continue;
                        }

                        Map<String, String> row = new LinkedHashMap<>();
                        List<CanalEntry.Column> columns;

                        if (eventType == CanalEntry.EventType.DELETE) {
                            columns = rowData.getBeforeColumnsList();
                        } else {
                            columns = rowData.getAfterColumnsList();
                        }

                        for (CanalEntry.Column column : columns) {
                            if (!hasInitPkNames && column.getIsKey()) {
                                flatMessage.addPkName(column.getName());
                            }
                            sqlType.put(column.getName(), column.getSqlType());
                            mysqlType.put(column.getName(), column.getMysqlType());
                            if (column.getIsNull()) {
                                row.put(column.getName(), null);
                            } else {
                                row.put(column.getName(), column.getValue());
                            }
                            // 获取update为true的字段
                            if (column.getUpdated()) {
                                updateSet.add(column.getName());
                            }
                        }

                        hasInitPkNames = true;
                        if (!row.isEmpty()) {
                            data.add(row);
                        }

                        if (eventType == CanalEntry.EventType.UPDATE) {
                            Map<String, String> rowOld = new LinkedHashMap<>();
                            for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                                if (updateSet.contains(column.getName())) {
                                    if (column.getIsNull()) {
                                        rowOld.put(column.getName(), null);
                                    } else {
                                        rowOld.put(column.getName(), column.getValue());
                                    }
                                }
                            }
                            // update操作将记录修改前的值
                            if (!rowOld.isEmpty()) {
                                old.add(rowOld);
                            }
                        }
                    }
                    if (!sqlType.isEmpty()) {
                        flatMessage.setSqlType(sqlType);
                    }
                    if (!mysqlType.isEmpty()) {
                        flatMessage.setMysqlType(mysqlType);
                    }
                    if (!data.isEmpty()) {
                        flatMessage.setData(data);
                    }
                    if (!old.isEmpty()) {
                        flatMessage.setOld(old);
                    }
                }
            }
            return flatMessages;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将FlatMessage按指定的字段值hash拆分
     *
     * @param flatMessage flatMessage
     * @param partitionsNum 分区数量
     * @param pkHashConfigs hash映射
     * @return 拆分后的flatMessage数组
     */
    public static FlatMessage[] messagePartition(FlatMessage flatMessage, Integer partitionsNum, String pkHashConfigs) {
        if (partitionsNum == null) {
            partitionsNum = 1;
        }
        FlatMessage[] partitionMessages = new FlatMessage[partitionsNum];

        if (flatMessage.getIsDdl()) {
            partitionMessages[0] = flatMessage;
        } else {
            if (flatMessage.getData() != null && !flatMessage.getData().isEmpty()) {
                String database = flatMessage.getDatabase();
                String table = flatMessage.getTable();
                HashMode hashMode = getPartitionHashColumns(database + "." + table, pkHashConfigs);
                if (hashMode == null) {
                    // 如果都没有匹配，发送到第一个分区
                    partitionMessages[0] = flatMessage;
                } else if (hashMode.tableHash) {
                    int hashCode = table.hashCode();
                    int pkHash = Math.abs(hashCode) % partitionsNum;
                    // math.abs可能返回负值，这里再取反，把出现负值的数据还是写到固定的分区，仍然可以保证消费顺序
                    pkHash = Math.abs(pkHash);
                    partitionMessages[pkHash] = flatMessage;
                } else {
                    List<String> pkNames = hashMode.pkNames;
                    if (hashMode.autoPkHash) {
                        pkNames = flatMessage.getPkNames();
                    }

                    int idx = 0;
                    for (Map<String, String> row : flatMessage.getData()) {
                        int hashCode = database.hashCode();
                        if (pkNames != null) {
                            for (String pkName : pkNames) {
                                String value = row.get(pkName);
                                if (value == null) {
                                    value = "";
                                }
                                hashCode = hashCode ^ value.hashCode();
                            }
                        }

                        int pkHash = Math.abs(hashCode) % partitionsNum;
                        // math.abs可能返回负值，这里再取反，把出现负值的数据还是写到固定的分区，仍然可以保证消费顺序
                        pkHash = Math.abs(pkHash);

                        FlatMessage flatMessageTmp = partitionMessages[pkHash];
                        if (flatMessageTmp == null) {
                            flatMessageTmp = new FlatMessage(flatMessage.getId());
                            partitionMessages[pkHash] = flatMessageTmp;
                            flatMessageTmp.setDatabase(flatMessage.getDatabase());
                            flatMessageTmp.setTable(flatMessage.getTable());
                            flatMessageTmp.setIsDdl(flatMessage.getIsDdl());
                            flatMessageTmp.setType(flatMessage.getType());
                            flatMessageTmp.setSql(flatMessage.getSql());
                            flatMessageTmp.setSqlType(flatMessage.getSqlType());
                            flatMessageTmp.setMysqlType(flatMessage.getMysqlType());
                            flatMessageTmp.setEs(flatMessage.getEs());
                            flatMessageTmp.setTs(flatMessage.getTs());
                            flatMessageTmp.setPkNames(flatMessage.getPkNames());
                        }
                        List<Map<String, String>> data = flatMessageTmp.getData();
                        if (data == null) {
                            data = new ArrayList<>();
                            flatMessageTmp.setData(data);
                        }
                        data.add(row);
                        if (flatMessage.getOld() != null && !flatMessage.getOld().isEmpty()) {
                            List<Map<String, String>> old = flatMessageTmp.getOld();
                            if (old == null) {
                                old = new ArrayList<>();
                                flatMessageTmp.setOld(old);
                            }
                            old.add(flatMessage.getOld().get(idx));
                        }
                        idx++;
                    }
                }
            } else {
                // 针对stmt/mixed binlog格式的query事件
                partitionMessages[0] = flatMessage;
            }
        }
        return partitionMessages;
    }

    /**
     * match return List , not match return null
     */
    public static HashMode getPartitionHashColumns(String name, String pkHashConfigs) {
        if (StringUtils.isEmpty(pkHashConfigs)) {
            return null;
        }

        List<PartitionData> datas = partitionDatas.get(pkHashConfigs);
        for (PartitionData data : datas) {
            if (data.simpleName != null) {
                if (data.simpleName.equalsIgnoreCase(name)) {
                    return data.hashMode;
                }
            } else {
                if (data.regexFilter.filter(name)) {
                    return data.hashMode;
                }
            }
        }

        return null;
    }

    private static Set<String> matchTopics(String name, String dynamicTopicConfigs) {
        String[] router = StringUtils.split(StringUtils.replace(dynamicTopicConfigs, ",", ";"), ";");
        Set<String> topics = new HashSet<>();
        for (String item : router) {
            int i = item.indexOf(":");
            if (i > -1) {
                String topic = item.substring(0, i).trim();
                String topicConfigs = item.substring(i + 1).trim();
                if (matchDynamicTopic(name, topicConfigs)) {
                    topics.add(topic);
                }
            } else if (matchDynamicTopic(name, item)) {
                topics.add(name.toLowerCase());
            }
        }
        return topics.isEmpty() ? null : topics;
    }

    public static boolean matchDynamicTopic(String name, String dynamicTopicConfigs) {
        if (StringUtils.isEmpty(dynamicTopicConfigs)) {
            return false;
        }

        boolean res = false;
        List<DynamicTopicData> datas = dynamicTopicDatas.get(dynamicTopicConfigs);
        for (DynamicTopicData data : datas) {
            if (data.simpleName != null) {
                if (data.simpleName.equalsIgnoreCase(name)) {
                    res = true;
                    break;
                }
            } else if (name.contains(".")) {
                if (data.tableRegexFilter != null && data.tableRegexFilter.filter(name)) {
                    res = true;
                    break;
                }
            } else {
                if (data.schemaRegexFilter != null && data.schemaRegexFilter.filter(name)) {
                    res = true;
                    break;
                }
            }
        }
        return res;
    }

    public static boolean checkPkNamesHasContain(List<String> pkNames, String name) {
        for (String pkName : pkNames) {
            if (pkName.equalsIgnoreCase(name)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isWildCard(String value) {
        // not contaiins '.' ?
        return StringUtils.containsAny(value, new char[] { '*', '?', '+', '|', '(', ')', '{', '}', '[', ']', '\\', '$',
                '^' });
    }

    private static void put2MapMessage(Map<String, Message> messageMap, Long messageId, String topicName,
                                       CanalEntry.Entry entry) {
        Message message = messageMap.get(topicName);
        if (message == null) {
            message = new Message(messageId, new ArrayList<CanalEntry.Entry>());
            messageMap.put(topicName, message);
        }
        message.getEntries().add(entry);
    }

    public static class PartitionData {

        public String             simpleName;
        public AviaterRegexFilter regexFilter;
        public HashMode           hashMode = new HashMode();
    }

    public static class HashMode {

        public boolean      autoPkHash = false;
        public boolean      tableHash  = false;
        public List<String> pkNames    = Lists.newArrayList();
    }

    public static class DynamicTopicData {

        public String             simpleName;
        public AviaterRegexFilter schemaRegexFilter;
        public AviaterRegexFilter tableRegexFilter;
    }
}

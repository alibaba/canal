package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.exception.CacheConnectionNull;
import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.exception.NoHistoryException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.*;

public class HistoryTableMetaCache {
    private TableMetaStorage tableMetaStorage;
    private MysqlConnection metaConnection;
    private LoadingCache<String, Map<Long, TableMeta>> cache; // 第一层：数据库名.表名，第二层时间戳，TableMeta

    public HistoryTableMetaCache() {
        cache = CacheBuilder.newBuilder().build(new CacheLoader<String, Map<Long, TableMeta>>() {
            @Override
            public Map<Long, TableMeta> load(String tableName) throws Exception {
                Long timestamp = new Date().getTime();
                String[] strs = tableName.split("\\.");
                String schema = strs[0];
                if (tableMetaStorage != null) {
                    init(tableMetaStorage.fetchByTableName(tableName)); // 从存储中读取表的历史ddl
                }
                ResultSetPacket resultSetPacket = connectionQuery("show create table " + tableName); // 获取当前ddl
                String currentDdl = resultSetPacket.getFieldValues().get(1);
                if (cache.asMap().containsKey(tableName)) {
                    Map<Long, TableMeta> tableMetaMap = cache.getUnchecked(tableName);
                    if (tableMetaMap.isEmpty()) {
                        put(schema, tableName, currentDdl, timestamp - 1000L); // 放入当前schema，取时间为当前时间-1s
                    } else {                                               // 如果table存在历史
                        Iterator<Long> iterator = tableMetaMap.keySet().iterator();
                        Long firstTimestamp = iterator.next();
                        TableMeta first = tableMetaMap.get(firstTimestamp); // 拿第一条ddl
                        if (!first.getDdl().equalsIgnoreCase(currentDdl)) { // 当前ddl与历史第一条不一致，放入当前ddl
                            put(schema, tableName, currentDdl, calculateNewTimestamp(firstTimestamp)); // 计算放入的timestamp,设为第一条时间+1s
                        }
                    }
                } else {
                    put(schema, tableName, currentDdl, timestamp - 1000L); // 放入当前schema
                }
                return cache.get(tableName);
            }
        });
    }

    public void init(List<TableMetaEntry> entries) throws IOException {
        for (TableMetaEntry entry : entries) {
            try {
                put(entry.getSchema(), entry.getTable(), entry.getDdl(), entry.getTimestamp());
            } catch (CacheConnectionNull cacheConnectionNull) {
                cacheConnectionNull.printStackTrace();
            }
        }
    }

    public TableMeta put(String schema, String table, String ddl, Long timestamp) throws CacheConnectionNull, IOException {
        ResultSetPacket resultSetPacket;
        if (!(ddl.contains("CREATE TABLE") || ddl.contains("create table"))) { // 尝试直接从数据库拉取CREATE TABLE的DDL
            resultSetPacket = connectionQuery("show create table " + table);
            ddl = resultSetPacket.getFieldValues().get(1);
        } else { // CREATE TABLE 的 DDL
            resultSetPacket = new ResultSetPacket();
            List<String> fields = new ArrayList<String>();
            String[] strings = table.split("\\.");
            String shortTable = table;
            if (strings.length > 1) {
                shortTable = strings[1];
            }
            fields.add(0, shortTable);
            fields.add(1, ddl);
            resultSetPacket.setFieldValues(fields);
            if (metaConnection != null) {
                resultSetPacket.setSourceAddress(metaConnection.getAddress());
            }
        }
        Map<Long, TableMeta> tableMetaMap;
        if (!cache.asMap().containsKey(table)) {
            tableMetaMap = new TreeMap<Long, TableMeta>(new Comparator<Long>() {
                @Override
                public int compare(Long o1, Long o2) {
                    return o2.compareTo(o1);
                }
            });
            cache.put(table, tableMetaMap);
        } else {
            tableMetaMap = cache.getUnchecked(table);
        }
        eliminate(tableMetaMap); // 淘汰旧的TableMeta
        TableMeta tableMeta = new TableMeta(schema, table, TableMetaCache.parseTableMeta(schema, table, resultSetPacket));
        if (tableMeta.getDdl() == null) { // 生成的TableMeta有时DDL为null
            tableMeta.setDdl(ddl);
        }
        tableMetaMap.put(timestamp, tableMeta);
        return tableMeta;
    }

    public TableMeta get(String schema, String table, Long timestamp) throws NoHistoryException, CacheConnectionNull {
        Map<Long, TableMeta> tableMetaMap = cache.getUnchecked(table);
        Iterator<Long> iterator = tableMetaMap.keySet().iterator();
        Long selected = null;
        while(iterator.hasNext()) {
            Long temp = iterator.next();
            if (timestamp > temp) {
                selected = temp;
                break;
            }
        }

        if (selected == null) {
            iterator = tableMetaMap.keySet().iterator();
            if (iterator.hasNext()) {
                selected = iterator.next();
            } else {
                throw new NoHistoryException(schema, table);
            }
        }

        return tableMetaMap.get(selected);
    }

    public void clearTableMeta() {
        cache.invalidateAll();
    }

    public void clearTableMetaWithSchemaName(String schema) {
        for (String tableName : cache.asMap().keySet()) {
            String[] strs = tableName.split("\\.");
            if (schema.equalsIgnoreCase(strs[0])) {
                cache.invalidate(tableName);
            }
        }
    }

    public void clearTableMeta(String schema, String table) {
        if (!table.contains(".")) {
            table = schema+"."+table;
        }
        cache.invalidate(table);
    }

    // eliminate older table meta in cache
    private void eliminate(Map<Long, TableMeta> tableMetaMap) {
        int MAX_CAPABILITY = 20;
        if (tableMetaMap.keySet().size() < MAX_CAPABILITY) {
            return;
        }
        Iterator<Long> iterator = tableMetaMap.keySet().iterator();
        while(iterator.hasNext()) {
            iterator.next();
        }
        iterator.remove();
    }

    private Long calculateNewTimestamp(Long oldTimestamp) {
        return oldTimestamp + 1000;
    }

    private ResultSetPacket connectionQuery(String query) throws CacheConnectionNull, IOException {
        if (metaConnection == null) {
            throw new CacheConnectionNull();
        }
        try {
            return metaConnection.query(query);
        } catch (IOException e) {
            try {
                metaConnection.reconnect();
                return metaConnection.query(query);
            } catch (IOException e1) {
                throw e1;
            }
        }
    }

    public void setMetaConnection(MysqlConnection metaConnection) {
        this.metaConnection = metaConnection;
    }

    public void setTableMetaStorage(TableMetaStorage tableMetaStorage) {
        this.tableMetaStorage = tableMetaStorage;
    }
}

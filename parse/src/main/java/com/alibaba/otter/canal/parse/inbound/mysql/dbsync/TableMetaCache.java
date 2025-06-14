package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.MemoryTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * 处理table meta解析和缓存
 *
 * @author jianghang 2013-1-17 下午10:15:16
 * @version 1.0.0
 */
public class TableMetaCache {

    public static final String              COLUMN_NAME    = "field";
    public static final String              COLUMN_TYPE    = "type";
    public static final String              IS_NULLABLE    = "null";
    public static final String              COLUMN_KEY     = "key";
    public static final String              COLUMN_DEFAULT = "default";
    public static final String              EXTRA          = "extra";
    private MysqlConnection                 connection;
    private boolean                         isOnRDS        = false;
    private boolean                         isOnPolarX     = false;
    private boolean                         isOnTSDB       = false;

    private TableMetaTSDB                   tableMetaTSDB;

    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private LoadingCache<String, TableMeta> tableMetaCache;

    public TableMetaCache(MysqlConnection con, TableMetaTSDB tableMetaTSDB){
        this.connection = con;
        this.tableMetaTSDB = tableMetaTSDB;
        // 如果持久存储的表结构为空，从db里面获取下
        if (tableMetaTSDB == null) {
            this.tableMetaCache = createLocalCache();
        } else {
            isOnTSDB = true;
        }
        try {
            ResultSetPacket packet = connection.query("show global variables  like 'rds\\_%'");
            // if (packet.getFieldValues().size() > 0) {
            isOnRDS = packet.getFieldValues().size() > 0;
            // }
        } catch (IOException e) {
        }
        try {
            ResultSetPacket packet = connection.query("show global variables  like 'polarx\\_%'");
            // if (packet.getFieldValues().size() > 0) {
            isOnPolarX = packet.getFieldValues().size() > 0;
            // }
        } catch (IOException e) {
        }
    }

    private LoadingCache<String, TableMeta> createLocalCache() {
        return CacheBuilder.newBuilder().build(new CacheLoader<String, TableMeta>() {

            @Override
            public TableMeta load(String name) throws Exception {
                try {
                    return getTableMetaByDB(name);
                } catch (Throwable e) {
                    // 尝试做一次retry操作
                    try {
                        connection.reconnect();
                        return getTableMetaByDB(name);
                    } catch (IOException e1) {
                        throw new CanalParseException("fetch table meta failed. table: " + name, e1);
                    }
                }
            }
        });
    }

    private /* synchronized */ TableMeta getTableMetaByDB(String fullname) throws IOException {
        // try {
        //    ResultSetPacket packet = connection.query("show create table " + fullname);
        //    String[] names = StringUtils.split(fullname, "`.`");
        //    String schema = names[0];
        //    String table = names[1].substring(0, names[1].length());
        //    return new TableMeta(schema, table, parseTableMeta(schema, table, packet));
        // } catch (Throwable e) { // fallback to desc table
        //    ResultSetPacket packet = connection.query("desc " + fullname);
        //    String[] names = StringUtils.split(fullname, "`.`");
        //    String schema = names[0];
        //    String table = names[1].substring(0, names[1].length());
        //    return new TableMeta(schema, table, parseTableMetaByDesc(packet));
        // }
        boolean showCreateTable = true;
        ResultSetPacket packet = null;
        synchronized (this) {
            try {
                packet = connection.query("show create table " + fullname);
            }catch (Exception ex) {
                packet = connection.query("desc " + fullname);
            }
        }
        String[] names = StringUtils.split(fullname, "`.`");
        String schema = names[0];
        String table = names[1].substring(0, names[1].length());
        List<FieldMeta> fieldMetas = showCreateTable ? parseTableMeta(schema, table, packet) : parseTableMetaByDesc(packet);
        return new TableMeta(schema, table, fieldMetas);
    }

    public static List<FieldMeta> parseTableMeta(String schema, String table, ResultSetPacket packet) {
        if (packet.getFieldValues().size() > 1) {
            String createDDL = packet.getFieldValues().get(1);
            MemoryTableMeta memoryTableMeta = new MemoryTableMeta();
            memoryTableMeta.apply(DatabaseTableMeta.INIT_POSITION, schema, createDDL, null);
            TableMeta tableMeta = memoryTableMeta.find(schema, table);
            return tableMeta.getFields();
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * 处理desc table的结果
     */
    public static List<FieldMeta> parseTableMetaByDesc(ResultSetPacket packet) {
        List<FieldPacket> fieldPackets = packet.getFieldDescriptors();
        int size = fieldPackets.size();
        Map<String, Integer> nameMaps = new HashMap<>(size);
        int index = 0;
        // for (FieldPacket fieldPacket : packet.getFieldDescriptors()) {
        for (FieldPacket fieldPacket : fieldPackets) {
            nameMaps.put(StringUtils.lowerCase(fieldPacket.getName()), index++);
        }

        List<String> fieldValues = packet.getFieldValues();
        // int size = packet.getFieldDescriptors().size();
        // int count = packet.getFieldValues().size() / packet.getFieldDescriptors().size();
        int count = fieldValues.size() / size;
        List<FieldMeta> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            FieldMeta meta = new FieldMeta();
            // 做一个优化，使用String.intern()，共享String对象，减少内存使用
            meta.setColumnName(fieldValues.get(nameMaps.get(COLUMN_NAME) + i * size).intern());
            meta.setColumnType(fieldValues.get(nameMaps.get(COLUMN_TYPE) + i * size));
            meta.setNullable(StringUtils.equalsIgnoreCase(fieldValues.get(nameMaps.get(IS_NULLABLE) + i * size), "YES"));
            meta.setKey("PRI".equalsIgnoreCase(fieldValues.get(nameMaps.get(COLUMN_KEY) + i * size)));
            meta.setUnique("UNI".equalsIgnoreCase(fieldValues.get(nameMaps.get(COLUMN_KEY) + i * size)));
            // 特殊处理引号
            meta.setDefaultValue(DruidDdlParser.unescapeQuotaName(fieldValues.get(nameMaps.get(COLUMN_DEFAULT) + i * size)));
            meta.setExtra(fieldValues.get(nameMaps.get(EXTRA) + i * size));
            result.add(meta);
        }
        return result;
    }

    // This method is unused
    // public TableMeta getTableMeta(String schema, String table) {
    //    return getTableMeta(schema, table, true);
    // }

    // This method is unused
    // public TableMeta getTableMeta(String schema, String table, boolean useCache) {
    //    String fullName = getFullName(schema, table);
    //    if (!useCache) {
    //        // tableMetaCache.invalidate(getFullName(schema, table));
    //        tableMetaCache.invalidate(fullName);
    //    }
    //    // return tableMetaCache.getUnchecked(getFullName(schema, table));
    //    return tableMetaCache.getUnchecked(fullName);
    // }

    // This method is unused
    // public TableMeta getTableMeta(String schema, String table, EntryPosition position) {
    //    return getTableMeta(schema, table, true, position);
    // }

    public /* synchronized */ TableMeta getTableMeta(String schema, String table, boolean useCache, EntryPosition position) {
        TableMeta tableMeta = null;
        if (tableMetaTSDB != null) {
            tableMeta = tableMetaTSDB.find(schema, table);
            if (tableMeta == null) {
                // 因为条件变化，可能第一次的tableMeta没取到，需要从db获取一次，并记录到snapshot中
                String fullName = getFullName(schema, table);
                try {
                    synchronized (this) {
                        tableMeta = tableMetaTSDB.find(schema, table);
                        if (tableMeta != null) {
                            return tableMeta;
                        }

                        ResultSetPacket packet = null;
                        try {
                            packet = connection.query("show create table " + fullName);
                        } catch (Exception e) {
                            // 尝试做一次retry操作
                            connection.reconnect();
                            packet = connection.query("show create table " + fullName);
                        }

                        String createDDL = null;
                        if (packet.getFieldValues().size() > 0) {
                            createDDL = packet.getFieldValues().get(1);
                        }
                        // 强制覆盖掉内存值
                        tableMetaTSDB.apply(position, schema, createDDL, "first");
                        tableMeta = tableMetaTSDB.find(schema, table);
                    }
                } catch (IOException e) {
                    throw new CanalParseException("fetch failed by table meta:" + fullName, e);
                }
            }
            return tableMeta;
        } else {
            String fullName = getFullName(schema, table);
            if (!useCache) {
                // tableMetaCache.invalidate(getFullName(schema, table));
                tableMetaCache.invalidate(fullName);
            }
            // return tableMetaCache.getUnchecked(getFullName(schema, table));
            return tableMetaCache.getUnchecked(fullName);
        }
    }

    public void clearTableMeta(String schema, String table) {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaCache.invalidate(getFullName(schema, table));
        }
    }

    public void clearTableMetaWithSchemaName(String schema) {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            ConcurrentMap<String, TableMeta> map =  tableMetaCache.asMap();
            if (!map.isEmpty()) {
                for (String name : map.keySet()) {
                    if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
                        // removeNames.add(name);
                        tableMetaCache.invalidate(name);
                    }
                }
            }
        }
    }

    public void clearTableMeta() {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaCache.invalidateAll();
        }
    }

    /**
     * 更新一下本地的表结构内存
     *
     * @param position
     * @param schema
     * @param ddl
     * @return
     */
    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        if (tableMetaTSDB != null) {
            return tableMetaTSDB.apply(position, schema, ddl, extra);
        } else {
            // ignore
            return true;
        }
    }

    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder(64);
        return builder.append('`').append(schema).append('`')
            .append('.')
            .append('`').append(StringUtils.replace(table,"`","``")).append('`')
            .toString();
    }

    public boolean isOnTSDB() {
        return isOnTSDB;
    }

    public void setOnTSDB(boolean isOnTSDB) {
        this.isOnTSDB = isOnTSDB;
    }

    public boolean isOnRDS() {
        return isOnRDS;
    }

    public void setOnRDS(boolean isOnRDS) {
        this.isOnRDS = isOnRDS;
    }

    public boolean isOnPolarX() {
        return isOnPolarX;
    }

    public void setOnPolarX(boolean isOnPolarX) {
        this.isOnPolarX = isOnPolarX;
    }
}

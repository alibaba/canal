package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.dbsync.binlog.BinlogPosition;
import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.ProcessJdbcResult;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaManager;

/**
 * 处理table meta解析和缓存
 *
 * @author jianghang 2013-1-17 下午10:15:16
 * @version 1.0.0
 */
public class TableMetaCache {

    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String COLUMN_TYPE = "COLUMN_TYPE";
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String COLUMN_KEY = "COLUMN_KEY";
    public static final String COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String EXTRA = "EXTRA";
    private MysqlConnection connection;
    private boolean isOnRDS = false;

    private TableMetaManager tableMetaManager;
    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private LoadingCache<String, TableMeta> tableMetaDB;


    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private Map<String, TableMeta> tableMetaCache;

    /**
     * 从db获取表结构
     * @param fullname
     * @return
     */
    private TableMeta getTableMetaByDB(final String fullname) {
        return connection.query("desc " + fullname, new ProcessJdbcResult<TableMeta>() {

            @Override
            public TableMeta process(ResultSet rs) throws SQLException {
                List<FieldMeta> metas = new ArrayList<FieldMeta>();
                while (rs.next()) {
                    FieldMeta meta = new FieldMeta();
                    // 做一个优化，使用String.intern()，共享String对象，减少内存使用
                    meta.setColumnName(rs.getString("Field"));
                    meta.setColumnType(rs.getString("Type"));
                    meta.setNullable(StringUtils.equalsIgnoreCase(rs.getString("Null"), "YES"));
                    meta.setKey("PRI".equalsIgnoreCase(rs.getString("Key")));
                    meta.setDefaultValue(rs.getString("Default"));
                    metas.add(meta);
                }

                String[] names = StringUtils.split(fullname, "`.`");
                String schema = names[0];
                String table = names[1].substring(0, names[1].length());
                return new TableMeta(schema, table, metas);
            }
        });
    }

    public TableMetaCache(MysqlConnection con,TableMetaManager tableMetaManager) {
        this.connection = con;
        this.tableMetaManager = tableMetaManager;
        //如果持久存储的表结构为空，从db里面获取下
        if (tableMetaManager == null) {
            this.tableMetaDB = CacheBuilder.newBuilder().build(new CacheLoader<String, TableMeta>() {

                @Override
                public TableMeta load(String name) throws Exception {
                    try {
                        return getTableMetaByDB(name);
                    } catch (CanalParseException e) {
                        // 尝试做一次retry操作
                        try {
                            connection.reconnect();
                            return getTableMetaByDB(name);
                        } catch (IOException e1) {
                            throw new CanalParseException("fetch failed by table meta:" + name, e1);
                        }
                    }
                }

            });
        }

        isOnRDS = connection.query("show global variables  like 'rds\\_%'", new ProcessJdbcResult<Boolean>() {

            @Override
            public Boolean process(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return true;
                }
                return false;
            }
        });
    }

    public TableMeta getTableMeta(String schema, String table, BinlogPosition position) {
        return getTableMeta(schema, table, true, position);
    }

    public TableMeta getTableMeta(String schema, String table, boolean useCache, BinlogPosition position) {
        TableMeta tableMeta = null;
        if (tableMetaManager != null) {
            tableMeta = tableMetaManager.find(schema, table);
            if (tableMeta == null) {
                // 因为条件变化，可能第一次的tableMeta没取到，需要从db获取一次，并记录到snapshot中
                String createDDL = connection.query("show create table " + getFullName(schema, table),
                    new ProcessJdbcResult<String>() {

                        @Override
                        public String process(ResultSet rs) throws SQLException {
                            while (rs.next()) {
                                return rs.getString(2);
                            }
                            return null;
                        }
                    });
                // 强制覆盖掉内存值
                tableMetaManager.apply(position, schema, createDDL);
                tableMeta = tableMetaManager.find(schema, table);
            }
            return tableMeta;
        } else {
            if (!useCache) {
                tableMetaDB.invalidate(getFullName(schema, table));
            }

            return tableMetaDB.getUnchecked(getFullName(schema, table));
        }
    }



    public void clearTableMeta(String schema, String table) {
        if (tableMetaManager != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaDB.invalidate(getFullName(schema, table));
        }
    }

    public void clearTableMetaWithSchemaName(String schema) {
        if (tableMetaManager != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            for (String name : tableMetaDB.asMap().keySet()) {
                if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
                    // removeNames.add(name);
                    tableMetaDB.invalidate(name);
                }
            }
        }
    }

    public void clearTableMeta() {
        if (tableMetaManager != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaDB.invalidateAll();
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
    public boolean apply(BinlogPosition position, String schema, String ddl) {
        if (tableMetaManager != null) {
            return tableMetaManager.apply(position, schema, ddl);
        } else {
            // ignore
            return true;
        }
    }



    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        return builder.append('`')
            .append(schema)
            .append('`')
            .append('.')
            .append('`')
            .append(table)
            .append('`')
            .toString();
    }

    public boolean isOnRDS() {
        return isOnRDS;
    }

    public void setOnRDS(boolean isOnRDS) {
        this.isOnRDS = isOnRDS;
    }

}

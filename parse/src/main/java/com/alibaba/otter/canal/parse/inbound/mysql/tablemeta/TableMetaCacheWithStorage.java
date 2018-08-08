package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta;

import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TableMetaCacheWithStorage implements TableMetaCacheInterface {

    private static Logger logger = LoggerFactory.getLogger(TableMetaCacheWithStorage.class);
    private TableMetaStorage tableMetaStorage; // TableMeta存储
    private HistoryTableMetaCache cache = new HistoryTableMetaCache(); // cache

    public TableMetaCacheWithStorage(MysqlConnection con, TableMetaStorage tableMetaStorage) {
        this.tableMetaStorage = tableMetaStorage;
        InetSocketAddress address = con.getAddress();
        this.tableMetaStorage.setDbAddress(address.getHostName()+":"+address.getPort());
        cache.setMetaConnection(con);
        cache.setTableMetaStorage(tableMetaStorage);
        if (tableMetaStorage != null) {
            try {
                cache.init(tableMetaStorage.fetch()); // 初始化，从存储拉取TableMeta
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public boolean apply(EntryPosition position, String fullTableName, String ddl, String extra) {
        String[] strs = fullTableName.split("\\.");
        String schema = strs[0];
        if (schema.equalsIgnoreCase("null")) { // ddl schema为null，放弃处理
            return false;
        }
        try {
            TableMeta tableMeta = cache.get(schema, fullTableName, position.getTimestamp());
            if (!compare(tableMeta, ddl)) { // 获取最近的TableMeta，进行比对
                TableMeta result = cache.put(schema, fullTableName, ddl, calTimestamp(position.getTimestamp()));
                if (tableMetaStorage != null && result != null) { // 储存
                    tableMetaStorage.store(schema, fullTableName, result.getDdl(), calTimestamp(position.getTimestamp()));
                }
            }
            return true;
        } catch (Exception e) {
            logger.error(e.toString());
        }

        return false;
    }

    @Override
    public boolean isOnRDS() {
        return false;
    }

    /***
     *
     * @param schema dbname
     * @param table tablename
     * @param useCache unused
     * @param position timestamp
     * @return
     */
    @Override
    public TableMeta getTableMeta(String schema, String table, boolean useCache, EntryPosition position) {
        String fulltbName = schema + "." + table;
        try {
            return cache.get(schema, fulltbName, position.getTimestamp());
        } catch (Exception e) {
            logger.error(e.toString());
        }
        return null;
    }

    @Override
    public void clearTableMeta() {
        cache.clearTableMeta();
    }

    @Override
    public void clearTableMetaWithSchemaName(String schema) {
        cache.clearTableMetaWithSchemaName(schema);
    }

    @Override
    public void clearTableMeta(String schema, String table) {
        cache.clearTableMeta(schema, table);
    }

    private boolean compare(TableMeta tableMeta, String ddl) {
        if (tableMeta == null) {
            return false;
        }
        return tableMeta.getDdl().equalsIgnoreCase(ddl);
    }

    private Long calTimestamp(Long timestamp) {
        return timestamp;
    }
}

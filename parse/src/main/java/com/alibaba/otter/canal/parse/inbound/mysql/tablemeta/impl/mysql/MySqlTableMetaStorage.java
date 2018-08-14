package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.impl.mysql;

import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.TableMetaEntry;
import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.TableMetaStorage;

import java.util.List;

public class MySqlTableMetaStorage implements TableMetaStorage {
    private MySqlTableMetaCallback mySqlTableMetaCallback;
    private String dbName;
    private String dbAddress;

    MySqlTableMetaStorage(MySqlTableMetaCallback callback, String dbName) {
        mySqlTableMetaCallback = callback;
        this.dbName = dbName;
    }


    @Override
    public void store(String schema, String table, String ddl, Long timestamp) {
        mySqlTableMetaCallback.save(dbAddress, schema, table, ddl, timestamp);
    }

    @Override
    public List<TableMetaEntry> fetch() {
        return mySqlTableMetaCallback.fetch(dbAddress, dbName);
    }

    @Override
    public List<TableMetaEntry> fetchByTableName(String tableName) {
        return mySqlTableMetaCallback.fetch(dbAddress, dbName, tableName);
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public String getDbAddress() {
        return dbAddress;
    }

    @Override
    public void setDbAddress(String address) {
        this.dbAddress = address;
    }
}

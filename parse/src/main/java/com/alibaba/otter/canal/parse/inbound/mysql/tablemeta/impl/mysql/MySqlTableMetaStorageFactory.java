package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.impl.mysql;

import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.TableMetaStorage;
import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.TableMetaStorageFactory;

public class MySqlTableMetaStorageFactory implements TableMetaStorageFactory {

    private MySqlTableMetaCallback mySQLTableMetaCallback;
    private String dbName;

    public MySqlTableMetaStorageFactory(MySqlTableMetaCallback callback, String dbName) {
        mySQLTableMetaCallback = callback;
        this.dbName = dbName;
    }

    @Override
    public TableMetaStorage getTableMetaStorage() {
        return new MySqlTableMetaStorage(mySQLTableMetaCallback, dbName);
    }

    @Override
    public String getDbName() {
        return dbName;
    }

}

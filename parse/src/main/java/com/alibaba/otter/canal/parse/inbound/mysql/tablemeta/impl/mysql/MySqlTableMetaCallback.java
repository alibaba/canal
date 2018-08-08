package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.impl.mysql;

import com.alibaba.otter.canal.parse.inbound.mysql.tablemeta.TableMetaEntry;

import java.util.List;

public interface MySqlTableMetaCallback {

    void save(String dbAddress, String schema, String table,String ddl, Long timestamp);

    List<TableMetaEntry> fetch(String dbAddress, String dbName);

    List<TableMetaEntry> fetch(String dbAddress, String dbName, String tableName);
}

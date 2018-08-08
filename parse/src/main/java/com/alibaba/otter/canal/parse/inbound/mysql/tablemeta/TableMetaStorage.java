package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta;

import java.util.List;

public interface TableMetaStorage {

    void store(String schema, String table, String ddl, Long timestamp);

    List<TableMetaEntry> fetch();

    List<TableMetaEntry> fetchByTableName(String tableName);

    String getDbName();

    String getDbAddress();

    void setDbAddress(String address);
}

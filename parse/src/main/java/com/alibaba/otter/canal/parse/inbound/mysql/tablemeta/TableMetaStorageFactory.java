package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta;

public interface TableMetaStorageFactory {

    TableMetaStorage getTableMetaStorage();

    String getDbName();

}

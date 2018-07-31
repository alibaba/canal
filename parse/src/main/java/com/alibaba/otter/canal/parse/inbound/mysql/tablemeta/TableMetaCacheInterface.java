package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta;

import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.protocol.position.EntryPosition;

public interface TableMetaCacheInterface {

    TableMeta getTableMeta(String schema, String table, boolean useCache, EntryPosition position);

    void clearTableMeta();

    void clearTableMetaWithSchemaName(String schema);

    void clearTableMeta(String schema, String table);

    boolean apply(EntryPosition position, String schema, String ddl, String extra);

    boolean isOnRDS();

}

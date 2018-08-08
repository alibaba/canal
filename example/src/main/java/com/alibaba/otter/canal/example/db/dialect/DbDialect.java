package com.alibaba.otter.canal.example.db.dialect;

import org.apache.ddlutils.model.Table;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.transaction.support.TransactionTemplate;

public interface DbDialect {

    LobHandler getLobHandler();

    JdbcTemplate getJdbcTemplate();

    TransactionTemplate getTransactionTemplate();

    Table findTable(String schema, String table);

    Table findTable(String schema, String table, boolean useCache);

}

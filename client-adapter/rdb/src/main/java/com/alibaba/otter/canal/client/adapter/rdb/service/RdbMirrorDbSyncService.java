package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public class RdbMirrorDbSyncService {

    private static final Logger        logger = LoggerFactory.getLogger(RdbMirrorDbSyncService.class);

    private Map<String, MappingConfig> mirrorDbConfigCache;                                           // 镜像库配置
    private DataSource                 dataSource;

    public RdbMirrorDbSyncService(Map<String, MappingConfig> mirrorDbConfigCache, DataSource dataSource,
                                  Integer threads){
        this.mirrorDbConfigCache = mirrorDbConfigCache;
        this.dataSource = dataSource;
    }

    public void sync(List<Dml> dmls) {
        for (Dml dml : dmls) {
            String destination = StringUtils.trimToEmpty(dml.getDestination());
            String database = dml.getDatabase();
            MappingConfig configMap = mirrorDbConfigCache.get(destination + "." + database);
            if (configMap == null) {
                continue;
            }
            if (dml.getSql() != null) {
                // DDL
                executeDdl(database, dml.getSql());
            } else {
                // DML
                // TODO
            }
        }
    }

    private void executeDdl(String database, String sql) {
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            statement.execute(sql);
            if (logger.isTraceEnabled()) {
                logger.trace("Execute DDL sql: {} for database: {}", sql, database);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}

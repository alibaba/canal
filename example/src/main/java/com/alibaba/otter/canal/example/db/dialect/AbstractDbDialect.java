/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.canal.example.db.dialect;

import com.alibaba.otter.canal.example.db.utils.DdlUtils;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.ddlutils.model.Table;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class AbstractDbDialect implements DbDialect {

    protected int databaseMajorVersion;
    protected int databaseMinorVersion;
    protected String databaseName;
    protected JdbcTemplate jdbcTemplate;
    protected TransactionTemplate transactionTemplate;
    protected LobHandler lobHandler;
    protected Map<List<String>, Table> tables;

    public AbstractDbDialect(final JdbcTemplate jdbcTemplate, LobHandler lobHandler) {
        this.jdbcTemplate = jdbcTemplate;
        this.lobHandler = lobHandler;
        // 初始化transction
        this.transactionTemplate = new TransactionTemplate();
        transactionTemplate.setTransactionManager(new DataSourceTransactionManager(jdbcTemplate.getDataSource()));
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        // 初始化一些数据
        jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData meta = c.getMetaData();
                databaseName = meta.getDatabaseProductName();
                databaseMajorVersion = meta.getDatabaseMajorVersion();
                databaseMinorVersion = meta.getDatabaseMinorVersion();

                return null;
            }
        });

        initTables(jdbcTemplate);
    }

    public Table findTable(String schema, String table, boolean useCache) {
        List<String> key = Arrays.asList(schema, table);
        if (useCache == false) {
            tables.remove(key);
        }

        return tables.get(key);
    }

    public Table findTable(String schema, String table) {
        return findTable(schema, table, true);
    }

    public LobHandler getLobHandler() {
        return lobHandler;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public TransactionTemplate getTransactionTemplate() {
        return transactionTemplate;
    }

    private void initTables(final JdbcTemplate jdbcTemplate) {
        this.tables = MigrateMap.makeComputingMap(new Function<List<String>, Table>() {

            public Table apply(List<String> names) {
                Assert.isTrue(names.size() == 2);
                try {
                    Table table = DdlUtils.findTable(jdbcTemplate, names.get(0), names.get(0), names.get(1));
                    if (table == null) {
                        throw new NestableRuntimeException("no found table [" + names.get(0) + "." + names.get(1)
                                + "] , pls check");
                    } else {
                        return table;
                    }
                } catch (Exception e) {
                    throw new NestableRuntimeException("find table [" + names.get(0) + "." + names.get(1) + "] error",
                            e);
                }
            }
        });
    }


}

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

package com.alibaba.otter.canal.example.db.dialect.mysql;

import com.alibaba.otter.canal.example.db.dialect.AbstractSqlTemplate;

/**
 * mysql sql生成模板
 *
 * @author jianghang 2011-10-27 下午01:41:20
 * @version 4.0.0
 */
public class MysqlSqlTemplate extends AbstractSqlTemplate {

    private static final String ESCAPE = "`";

    public String getMergeSql(String schemaName, String tableName, String[] pkNames, String[] columnNames,
                              String[] viewColumnNames, boolean includePks) {
        StringBuilder sql = new StringBuilder("insert into " + getFullName(schemaName, tableName) + "(");
        int size = columnNames.length;
        for (int i = 0; i < size; i++) {
            sql.append(appendEscape(columnNames[i])).append(" , ");
        }
        size = pkNames.length;
        for (int i = 0; i < size; i++) {
            sql.append(appendEscape(pkNames[i])).append((i + 1 < size) ? " , " : "");
        }

        sql.append(") values (");
        size = columnNames.length;
        for (int i = 0; i < size; i++) {
            sql.append("?").append(" , ");
        }
        size = pkNames.length;
        for (int i = 0; i < size; i++) {
            sql.append("?").append((i + 1 < size) ? " , " : "");
        }
        sql.append(")");
        sql.append(" on duplicate key update ");

        size = columnNames.length;
        for (int i = 0; i < size; i++) {
            sql.append(appendEscape(columnNames[i]))
                    .append("=values(")
                    .append(appendEscape(columnNames[i]))
                    .append(")");
            if (includePks) {
                sql.append(" , ");
            } else {
                sql.append((i + 1 < size) ? " , " : "");
            }
        }

        if (includePks) {
            // mysql merge sql匹配了uniqe / primary key时都会执行update，所以需要更新pk信息
            size = pkNames.length;
            for (int i = 0; i < size; i++) {
                sql.append(appendEscape(pkNames[i])).append("=values(").append(appendEscape(pkNames[i])).append(")");
                sql.append((i + 1 < size) ? " , " : "");
            }
        }

        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
    }

    protected String appendEscape(String columnName) {
        return ESCAPE + columnName + ESCAPE;
    }

}
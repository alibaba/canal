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

/**
 * 默认的基于标准SQL实现的CRUD sql封装
 * 
 * @author jianghang 2011-10-27 下午01:37:00
 * @version 4.0.0
 */
public abstract class AbstractSqlTemplate implements SqlTemplate {

    private static final String DOT = ".";

    public String getSelectSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder("select ");
        int size = columnNames.length;
        for (int i = 0; i < size; i++) {
            sql.append(appendEscape(columnNames[i])).append((i + 1 < size) ? " , " : "");
        }

        sql.append(" from ").append(getFullName(schemaName, tableName)).append(" where ( ");
        appendColumnEquals(sql, pkNames, "and");
        sql.append(" ) ");
        return sql.toString().intern();// 不使用intern，避免方法区内存消耗过多
    }

    public String getUpdateSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder("update " + getFullName(schemaName, tableName) + " set ");
        appendColumnEquals(sql, columnNames, ",");
        sql.append(" where (");
        appendColumnEquals(sql, pkNames, "and");
        sql.append(")");
        return sql.toString().intern(); // 不使用intern，避免方法区内存消耗过多
    }

    public String getInsertSql(String schemaName, String tableName, String[] pkNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder("insert into " + getFullName(schemaName, tableName) + "(");
        String[] allColumns = new String[pkNames.length + columnNames.length];
        System.arraycopy(columnNames, 0, allColumns, 0, columnNames.length);
        System.arraycopy(pkNames, 0, allColumns, columnNames.length, pkNames.length);

        int size = allColumns.length;
        for (int i = 0; i < size; i++) {
            sql.append(appendEscape(allColumns[i])).append((i + 1 < size) ? "," : "");
        }

        sql.append(") values (");
        appendColumnQuestions(sql, allColumns);
        sql.append(")");
        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
    }

    public String getDeleteSql(String schemaName, String tableName, String[] pkNames) {
        StringBuilder sql = new StringBuilder("delete from " + getFullName(schemaName, tableName) + " where ");
        appendColumnEquals(sql, pkNames, "and");
        return sql.toString().intern();// intern优化，避免出现大量相同的字符串
    }

    protected String getFullName(String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (schemaName != null) {
            sb.append(appendEscape(schemaName)).append(DOT);
        }
        sb.append(appendEscape(tableName));
        return sb.toString().intern();
    }

    // ================ helper method ============

    protected String appendEscape(String columnName) {
        return columnName;
    }

    protected void appendColumnQuestions(StringBuilder sql, String[] columns) {
        int size = columns.length;
        for (int i = 0; i < size; i++) {
            sql.append("?").append((i + 1 < size) ? " , " : "");
        }
    }

    protected void appendColumnEquals(StringBuilder sql, String[] columns, String separator) {
        int size = columns.length;
        for (int i = 0; i < size; i++) {
            sql.append(" ").append(appendEscape(columns[i])).append(" = ").append("? ");
            if (i != size - 1) {
                sql.append(separator);
            }
        }
    }
}

package com.alibaba.otter.canal.adapter.es.core.parser;

import static com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.parser.ParserException;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.Function;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.SqlItem;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.SqlItem.*;
import com.alibaba.otter.canal.adapter.es.core.util.Util;

public class SqlParser {

    public static SqlItem parse(Connection conn, String sql) {
        return parse(conn, sql, null);
    }

    public static SqlItem parse(Connection conn, String sql, String dataSourceKey) {
        try {
            if (StringUtils.isEmpty(sql)) {
                return null;
            }
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();

            SqlItem sqlItem = new SqlItem();

            // 解析select字段
            Map<String, FieldItem> fieldItems = sqlItem.getSelectFields();
            List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
            for (SQLSelectItem selectItem : selectItems) {
                FieldItem fieldItem = parseSelectField(selectItem);
                fieldItems.put(fieldItem.getFieldName(), fieldItem);
            }

            // 解析table
            SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
            parseTables(sqlTableSource, sqlItem, null);

            // 解析where条件
            WhereItem whereItem = new WhereItem();
            parseWhereExpr(whereItem, sqlSelectQueryBlock.getWhere());
            if (whereItem.getExpr() != null) {
                sqlItem.setWhereItem(whereItem);
            }

            // 解析group by语句
            SQLSelectGroupByClause groupByClause = sqlSelectQueryBlock.getGroupBy();
            if (groupByClause != null) {
                GroupByItem groupByItem = new GroupByItem(groupByClause);
                sqlItem.setGroupByItem(groupByItem);
            }

            Map<String, FieldItem> allColumns = new LinkedHashMap<>();
            // 检测是否有*号
            for (Iterator<Map.Entry<String, FieldItem>> it = sqlItem.getSelectFields().entrySet().iterator(); it
                .hasNext();) {
                FieldItem selectField = it.next().getValue();
                if (selectField.isRaw() && "*".equals(selectField.getRawColumn())) {
                    it.remove(); // 将该值删除
                    if (StringUtils.isEmpty(dataSourceKey)) {
                        throw new ParserException("Could not convert `*` to columns, because dataSourceKey is null.");
                    }
                    // 获取表名
                    TableItem tableItem = sqlItem.getTables().get(selectField.getRawOwner());
                    String metaSql = "SELECT * FROM `" + tableItem.getTableName() + "` LIMIT 1";
                    if (conn == null) {
                        throw new ParserException("DB connection is null");
                    }
                    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(metaSql)) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnCount = rsmd.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = rsmd.getColumnName(i);
                            FieldItem fieldItem = new FieldItem();
                            fieldItem.setFieldName(columnName);
                            SQLExpr sqlExpr;
                            if (StringUtils.isNotEmpty(selectField.getRawOwner())) {
                                sqlExpr = new SQLPropertyExpr(selectField.getRawOwner(), columnName);
                            } else {
                                sqlExpr = new SQLIdentifierExpr(columnName);
                            }
                            fieldItem.setExpr(sqlExpr);
                            ColumnItem columnItem = new ColumnItem();
                            columnItem.setOwner(selectField.getRawOwner());
                            columnItem.setColumnName(columnName);
                            fieldItem.getColumnItems().add(columnItem);
                            allColumns.put(columnName, fieldItem);
                        }
                    }
                }
            }
            if (!allColumns.isEmpty()) {
                sqlItem.getSelectFields().putAll(allColumns);
            }

            return sqlItem;
        } catch (ParserException e) {
            throw e;
        } catch (Exception e) {
            throw new ParserException();
        }
    }

    private static void parseTables(SQLTableSource sqlTableSource, SqlItem sqlItem, TableItem rightTable) {
        if (sqlTableSource instanceof SQLExprTableSource) {
            TableItem tableItem;
            if (rightTable != null) {
                tableItem = rightTable;
            } else {
                tableItem = new TableItem();
            }
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            tableItem.setSchema(sqlExprTableSource.getSchema());
            tableItem.setTableName(Util.cleanColumn(sqlExprTableSource.getTableName()));
            tableItem.setAlias(sqlExprTableSource.getAlias() == null ? "" : sqlExprTableSource.getAlias());
            sqlItem.getTables().put(tableItem.getAlias(), tableItem);
        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            if (!(rightTable instanceof SubQueryItem)) {
                throw new ParserException();
            }
            SubQueryItem subQueryItem = (SubQueryItem) rightTable;

            // 关联子查询表达式
            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            MySqlSelectQueryBlock sqlSelectQuery = (MySqlSelectQueryBlock) subQueryTableSource.getSelect().getQuery();

            subQueryItem.setAlias(subQueryTableSource.getAlias() == null ? "" : subQueryTableSource.getAlias());
            subQueryItem.setSubQueryBlock(sqlSelectQuery);
            // 只支持一层子查询
            if (!(sqlSelectQuery.getFrom() instanceof SQLExprTableSource)) {
                throw new UnsupportedOperationException("Unsupported for complex of sub query sql: " + sqlSelectQuery);
            }
            // 解析子查询的select
            List<SQLSelectItem> selectItems = sqlSelectQuery.getSelectList();
            List<FieldItem> fieldItems = subQueryItem.getSelectFields();
            for (SQLSelectItem selectItem : selectItems) {
                FieldItem fieldItem = parseSelectField(selectItem);
                fieldItems.add(fieldItem);
            }
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlSelectQuery.getFrom();
            subQueryItem.setSchema(sqlExprTableSource.getSchema());
            subQueryItem.setTableName(Util.cleanColumn(sqlExprTableSource.getTableName()));
            sqlItem.getTables().put(subQueryItem.getAlias(), subQueryItem);

            WhereItem whereItem = new WhereItem();
            parseWhereExpr(whereItem, sqlSelectQuery.getWhere());
            if (whereItem.getExpr() != null) {
                subQueryItem.setWhereItem(whereItem);
            }

            if (sqlSelectQuery.getGroupBy() != null) {
                GroupByItem groupByItem = new GroupByItem(sqlSelectQuery.getGroupBy());
                subQueryItem.setGroupByItem(groupByItem);
            }
        } else if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLTableSource leftTableSource = sqlJoinTableSource.getLeft(); // 左表
            parseTables(leftTableSource, sqlItem, null);

            SQLTableSource rightTableSource = sqlJoinTableSource.getRight(); // 右表
            TableItem rightTableItem;
            if (rightTableSource instanceof SQLSubqueryTableSource) {
                rightTableItem = new SubQueryItem();
            } else {
                rightTableItem = new TableItem();
            }

            if (sqlJoinTableSource.getJoinType() == SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN) {
                rightTableItem.setJoinType(TableItem.LEFT_JOIN);
            } else if (sqlJoinTableSource.getJoinType() == SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN) {
                rightTableItem.setJoinType(TableItem.RIGHT_JOIN);
            } else if (sqlJoinTableSource.getJoinType() == SQLJoinTableSource.JoinType.INNER_JOIN) {
                rightTableItem.setJoinType(TableItem.INNER_JOIN);
            }

            // 解析on表达
            parseOnExpr(sqlJoinTableSource.getCondition(), rightTableItem);

            parseTables(rightTableSource, sqlItem, rightTableItem);
        }
    }

    private static void parseOnExpr(SQLExpr expr, TableItem tableItem) {
        if (!(expr instanceof SQLBinaryOpExpr)) {
            throw new UnsupportedOperationException();
        }
        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
        if (sqlBinaryOpExpr.getOperator() == BooleanAnd) {
            parseOnExpr(sqlBinaryOpExpr.getLeft(), tableItem);
            parseOnExpr(sqlBinaryOpExpr.getRight(), tableItem);
        } else if (sqlBinaryOpExpr.getOperator() == Equality) {
            SQLExpr leftColumnExpr = sqlBinaryOpExpr.getLeft();
            FieldItem leftFieldItem = new FieldItem();
            Function.setFieldItem(leftColumnExpr, leftFieldItem);
            SQLExpr rightColumnExpr = sqlBinaryOpExpr.getRight();
            FieldItem rightFieldItem = new FieldItem();
            Function.setFieldItem(rightColumnExpr, rightFieldItem);
            // 只支持原生字段关联, 以提升查询性能减少解析复杂度
            if (!leftFieldItem.isRaw() || !rightFieldItem.isRaw()) {
                throw new UnsupportedOperationException(
                    "Unsupported for complex of 'on' expression: " + sqlBinaryOpExpr);
            }
            ConditionPair onFields = new ConditionPair(leftFieldItem, rightFieldItem);
            onFields.setExpr(sqlBinaryOpExpr);
            tableItem.getOnFields().add(onFields);
        } else {
            throw new UnsupportedOperationException("Unsupported for complex of 'on' expression: " + sqlBinaryOpExpr);
        }
    }

    private static void parseWhereExpr(WhereItem whereItem, SQLExpr expr) {
        if (expr == null) {
            return;
        }
        if (!(expr instanceof SQLBinaryOpExpr)) {
            throw new UnsupportedOperationException();
        }
        whereItem.setExpr(expr);
        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
        if (sqlBinaryOpExpr.getOperator() == BooleanAnd || sqlBinaryOpExpr.getOperator() == BooleanOr) {
            parseWhereExpr(whereItem, sqlBinaryOpExpr.getLeft());
            parseWhereExpr(whereItem, sqlBinaryOpExpr.getRight());
        } else {
            switch (sqlBinaryOpExpr.getOperator()) {
                case Equality:
                case GreaterThan:
                case GreaterThanOrEqual:
                case Is:
                case LessThan:
                case LessThanOrEqual:
                case LessThanOrGreater:
                case NotEqual:
                case NotLessThan:
                case NotGreaterThan:
                case IsNot:
                    // TODO 暂不适配Like
                    // case Like:
                    // case NotLike:
                    // case ILike:
                    // case NotILike:
                    break;
                default:
                    whereItem.setSimple(false);
                    break;
            }
            FieldItem leftField = new FieldItem();
            Function.setFieldItem(sqlBinaryOpExpr.getLeft(), leftField);
            FieldItem rightField = new FieldItem();
            Function.setFieldItem(sqlBinaryOpExpr.getRight(), rightField);
            whereItem.getColumnItems().addAll(leftField.getColumnItems());
            whereItem.getOwners().addAll(leftField.getOwners());
            if (!leftField.isSimple()) {
                whereItem.setSimple(false);
            }
            whereItem.getColumnItems().addAll(rightField.getColumnItems());
            whereItem.getOwners().addAll(rightField.getOwners());
            if (!rightField.isSimple()) {
                whereItem.setSimple(false);
            }
            if (whereItem.getOwners().size() > 1) {
                whereItem.setSimple(false);
            }
        }
    }

    private static FieldItem parseSelectField(SQLSelectItem selectItem) {
        FieldItem fieldItem = new FieldItem();
        Function.setFieldItem(selectItem.getExpr(), fieldItem);
        if (selectItem.getAlias() != null) {
            fieldItem.setFieldName(selectItem.getAlias());
        } else {
            // 未设置字段别名
            if (fieldItem.isRaw()) {
                // 原生字段则以字段名作为fieldName
                fieldItem.setFieldName(fieldItem.getRawColumn());
            } else {
                // 非原生字段则以全部表达式作为fieldName
                fieldItem.setFieldName(selectItem.toString());
            }
        }
        fieldItem.setExpr(selectItem.getExpr());
        return fieldItem;
    }
}

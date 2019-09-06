package com.alibaba.otter.canal.client.adapter.es.config;

import static com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator.BooleanAnd;
import static com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator.Equality;
import static com.alibaba.otter.canal.client.adapter.support.Constant.GROUP_BY_CONDITION_VARIANT;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.parser.ParserException;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.RelationFieldsPair;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.TableItem;

/**
 * ES同步指定sql格式解析
 *
 * @author rewerma 2018-10-26 下午03:45:49
 * @version 1.0.0
 */
public class SqlParser {

    /**
     * 解析sql
     *
     * @param sql sql
     * @return 视图对象
     */
    public static SchemaItem parse(String sql) {
        try {
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();

            SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
            List<TableItem> tableItems = new ArrayList<>();
            SqlParser.visitGroupByAddVariantRefExpr(sqlTableSource);
            SchemaItem schemaItem = new SchemaItem();
            schemaItem.setSql(SQLUtils.toMySqlString(sqlSelectQueryBlock));
            SqlParser.visitSelectTable(schemaItem, sqlTableSource, tableItems, null);
            tableItems.forEach(tableItem -> schemaItem.getAliasTableItems().put(tableItem.getAlias(), tableItem));

            List<FieldItem> fieldItems = collectSelectQueryFields(sqlSelectQueryBlock);
            fieldItems.forEach(fieldItem -> schemaItem.getSelectFields().put(fieldItem.getFieldName(), fieldItem));

            schemaItem.init();

            if (schemaItem.getAliasTableItems().isEmpty() || schemaItem.getSelectFields().isEmpty()) {
                throw new ParserException("Parse sql error");
            }
            return schemaItem;
        } catch (Exception e) {
            throw new ParserException();
        }
    }

    /**
     * 归集字段
     *
     * @param sqlSelectQueryBlock sqlSelectQueryBlock
     * @return 字段属性列表
     */
    private static List<FieldItem> collectSelectQueryFields(MySqlSelectQueryBlock sqlSelectQueryBlock) {
        return sqlSelectQueryBlock.getSelectList().stream().map(selectItem -> {
            FieldItem fieldItem = new FieldItem();
            fieldItem.setFieldName(selectItem.getAlias());
            fieldItem.setExpr(selectItem.toString());
            visitColumn(selectItem.getExpr(), fieldItem);
            return fieldItem;
        }).collect(Collectors.toList());
    }

    /**
     * 解析字段
     *
     * @param expr sql expr
     * @param fieldItem 字段属性
     */
    private static void visitColumn(SQLExpr expr, FieldItem fieldItem) {
        if (expr instanceof SQLIdentifierExpr) {
            // 无owner
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(identifierExpr.getName());
                fieldItem.setExpr(identifierExpr.toString());
            }
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(identifierExpr.getName());
            fieldItem.getOwners().add(null);
            fieldItem.addColumn(columnItem);
        } else if (expr instanceof SQLPropertyExpr) {
            // 有owner
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) expr;
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(sqlPropertyExpr.getName());
                fieldItem.setExpr(sqlPropertyExpr.toString());
            }
            fieldItem.getOwners().add(sqlPropertyExpr.getOwnernName());
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(sqlPropertyExpr.getName());
            columnItem.setOwner(sqlPropertyExpr.getOwnernName());
            fieldItem.addColumn(columnItem);
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
            fieldItem.setMethod(true);
            for (SQLExpr sqlExpr : methodInvokeExpr.getArguments()) {
                visitColumn(sqlExpr, fieldItem);
            }
        } else if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
            fieldItem.setBinaryOp(true);
            visitColumn(sqlBinaryOpExpr.getLeft(), fieldItem);
            visitColumn(sqlBinaryOpExpr.getRight(), fieldItem);
        } else if (expr instanceof SQLCaseExpr) {
            SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) expr;
            fieldItem.setMethod(true);
            sqlCaseExpr.getItems().forEach(item-> visitColumn(item.getConditionExpr(), fieldItem));
        }else if(expr instanceof SQLCharExpr) {
            SQLCharExpr sqlCharExpr = (SQLCharExpr) expr;
            String owner = null;
            String columnName = null;
            if (sqlCharExpr.getParent() instanceof SQLCaseExpr.Item) {
                owner = ((SQLPropertyExpr) ((SQLCaseExpr.Item) sqlCharExpr.getParent()).getValueExpr()).getOwnernName();
                columnName = ((SQLPropertyExpr) ((SQLCaseExpr.Item) sqlCharExpr.getParent()).getValueExpr()).getName();
            }
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(columnName);
                fieldItem.setExpr(sqlCharExpr.toString());
            }
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(columnName);
            columnItem.setOwner(owner);
            fieldItem.getOwners().add(owner);
            fieldItem.addColumn(columnItem);
        }
    }

    /**
     * 解析表
     *
     * @param schemaItem 视图对象
     * @param sqlTableSource sqlTableSource
     * @param tableItems 表对象列表
     * @param tableItemTmp 表对象(临时)
     */
    private static void visitSelectTable(SchemaItem schemaItem, SQLTableSource sqlTableSource,
                                         List<TableItem> tableItems, TableItem tableItemTmp) {
        if (sqlTableSource instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem(schemaItem);
            }
            tableItem.setSchema(sqlExprTableSource.getSchema());
            tableItem.setTableName(sqlExprTableSource.getTableName());
            if (tableItem.getAlias() == null) {
                tableItem.setAlias(sqlExprTableSource.getAlias());
            }
            if (tableItems.isEmpty()) {
                // 第一张表为主表
                tableItem.setMain(true);
            }
            tableItems.add(tableItem);
        } else if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLTableSource leftTableSource = sqlJoinTableSource.getLeft();
            visitSelectTable(schemaItem, leftTableSource, tableItems, null);
            SQLTableSource rightTableSource = sqlJoinTableSource.getRight();
            TableItem rightTableItem = new TableItem(schemaItem);
            // 解析on条件字段
            visitOnCondition(sqlJoinTableSource.getCondition(), rightTableItem);
            visitSelectTable(schemaItem, rightTableSource, tableItems, rightTableItem);

        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            MySqlSelectQueryBlock sqlSelectQuery = (MySqlSelectQueryBlock) subQueryTableSource.getSelect().getQuery();
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem(schemaItem);
            }
            tableItem.setAlias(subQueryTableSource.getAlias());
            tableItem.setSubQuerySql(SQLUtils.toMySqlString(sqlSelectQuery));
            tableItem.setSubQuery(true);
            tableItem.setSubQueryFields(collectSelectQueryFields(sqlSelectQuery));
            visitSelectTable(schemaItem, sqlSelectQuery.getFrom(), tableItems, tableItem);
        }
    }

    /**
     * 解析on条件
     *
     * @param expr sql expr
     * @param tableItem 表对象
     */
    private static void visitOnCondition(SQLExpr expr, TableItem tableItem) {
        if (!(expr instanceof SQLBinaryOpExpr)) {
            throw new UnsupportedOperationException();
        }
        SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
        if (sqlBinaryOpExpr.getOperator() == BooleanAnd) {
            visitOnCondition(sqlBinaryOpExpr.getLeft(), tableItem);
            visitOnCondition(sqlBinaryOpExpr.getRight(), tableItem);
        } else if (sqlBinaryOpExpr.getOperator() == Equality) {
            FieldItem leftFieldItem = new FieldItem();
            visitColumn(sqlBinaryOpExpr.getLeft(), leftFieldItem);
            if (leftFieldItem.getColumnItems().size() != 1 || leftFieldItem.isMethod() || leftFieldItem.isBinaryOp()) {
                throw new UnsupportedOperationException("Unsupported for complex of on-condition");
            }
            FieldItem rightFieldItem = new FieldItem();
            visitColumn(sqlBinaryOpExpr.getRight(), rightFieldItem);
            if (rightFieldItem.getColumnItems().size() != 1 || rightFieldItem.isMethod()
                || rightFieldItem.isBinaryOp()) {
                throw new UnsupportedOperationException("Unsupported for complex of on-condition");
            }
            tableItem.getRelationFields().add(new RelationFieldsPair(leftFieldItem, rightFieldItem));
        } else {
            throw new UnsupportedOperationException("Unsupported for complex of on-condition");
        }
    }

    /**
     * 解析sql group by 语句 增加占位符以使 自查询group by不需扫描全表
     * @param source
     */
    public static void visitGroupByAddVariantRefExpr(SQLTableSource source){
        if (source instanceof SQLJoinTableSource) {
            SQLTableSource left = ((SQLJoinTableSource) source).getLeft();
            visitGroupByAddVariantRefExpr(left);
            SQLTableSource right = ((SQLJoinTableSource) source).getRight();
            visitGroupByAddVariantRefExpr(right);
        }else if (source instanceof SQLSubqueryTableSource){
            SQLSelectQuery query = ((SQLSubqueryTableSource) source).getSelect().getQuery();
            SQLSelectGroupByClause groupBy = ((MySqlSelectQueryBlock) query).getGroupBy();
            SQLExpr condition = ((SQLJoinTableSource) source.getParent()).getCondition();

            String leftCondition = ((SQLPropertyExpr) ((SQLBinaryOpExpr) condition).getLeft()).getName();
            String rightCondition = ((SQLPropertyExpr) ((SQLBinaryOpExpr) condition).getRight()).getName();

            for (SQLExpr item : groupBy.getItems()) {
                String name = ((SQLIdentifierExpr) item).getName();
                if (name.equals(leftCondition)){
                    addConditionEl(query, name);
                }else if (name.equals(rightCondition)){
                    addConditionEl(query, name);
                }
            }

        }else {
        }
    }

    private static void addConditionEl(SQLSelectQuery query, String name) {
        SQLExpr whereSqlExpr = ((MySqlSelectQueryBlock) query).getWhere();
        if (whereSqlExpr == null) {
            SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
            sqlIdentifierExpr.setName(name);
            SQLVariantRefExpr sqlVariantRefExpr = new SQLVariantRefExpr();
            sqlVariantRefExpr.setName(GROUP_BY_CONDITION_VARIANT);
            sqlVariantRefExpr.setGlobal(false);
            sqlVariantRefExpr.setSession(false);
            sqlVariantRefExpr.setIndex(-1);
            SQLBinaryOpExpr sqlBinaryOpExpr = new SQLBinaryOpExpr(sqlIdentifierExpr, SQLBinaryOperator.Equality,sqlVariantRefExpr);
            sqlBinaryOpExpr.setDbType(((MySqlSelectQueryBlock) query).getDbType());
            ((MySqlSelectQueryBlock) query).setWhere(sqlBinaryOpExpr);
            sqlBinaryOpExpr.setParent(query);
        }else if (whereSqlExpr instanceof SQLBinaryOpExpr){
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) whereSqlExpr;
            SQLExpr rightChild = sqlBinaryOpExpr.getRight();
            SQLExpr parent = sqlBinaryOpExpr;
            while (rightChild instanceof SQLBinaryOpExpr){
                parent = rightChild;
                rightChild = ((SQLBinaryOpExpr) rightChild).getRight();
            }
            SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
            sqlIdentifierExpr.setName(name);
            SQLVariantRefExpr sqlVariantRefExpr = new SQLVariantRefExpr();
            sqlVariantRefExpr.setName(GROUP_BY_CONDITION_VARIANT);
            sqlVariantRefExpr.setGlobal(false);
            sqlVariantRefExpr.setSession(false);
            sqlVariantRefExpr.setIndex(-1);
            SQLBinaryOpExpr newCondition = new SQLBinaryOpExpr(sqlIdentifierExpr, SQLBinaryOperator.Equality,sqlVariantRefExpr);
            sqlBinaryOpExpr.setDbType(((MySqlSelectQueryBlock) query).getDbType());
            SQLBinaryOpExpr sqlBinaryOpExprParent =  (SQLBinaryOpExpr)parent.getParent();
            SQLBinaryOpExpr newParent = new SQLBinaryOpExpr(parent, BooleanAnd,newCondition);
            newParent.setDbType(((MySqlSelectQueryBlock) query).getDbType());
            sqlBinaryOpExprParent.setRight(newParent);
        }
    }

}

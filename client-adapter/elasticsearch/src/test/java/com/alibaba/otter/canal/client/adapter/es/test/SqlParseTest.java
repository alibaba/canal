package com.alibaba.otter.canal.client.adapter.es.test;

import java.util.List;
import java.util.Map;

import com.alibaba.fastsql.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQuery;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.es.config.SqlParser;

public class SqlParseTest {

    @Test
    public void parseTest() {
        String sql = "select a.id, CASE WHEN a.id <= 500 THEN '1' else '2' end as id2, "
                     + "concat(a.name,'_test') as name, a.role_id, b.name as role_name, c.labels from user a "
                     + "left join role b on a.role_id=b.id "
                     + "left join (select user_id, group_concat(label,',') as labels from user_label "
                     + "group by user_id) c on c.user_id=a.id";
        SchemaItem schemaItem = SqlParser.parse(sql);

        // 通过表名找 TableItem
        List<TableItem> tableItems = schemaItem.getTableItemAliases().get("user_label".toLowerCase());
        tableItems.forEach(tableItem -> Assert.assertEquals("c", tableItem.getAlias()));

        TableItem tableItem = tableItems.get(0);
        Assert.assertFalse(tableItem.isMain());
        Assert.assertTrue(tableItem.isSubQuery());
        // 通过字段名找 FieldItem
        List<FieldItem> fieldItems = schemaItem.getColumnFields().get(tableItem.getAlias() + ".labels".toLowerCase());
        fieldItems.forEach(
            fieldItem -> Assert.assertEquals("c.labels", fieldItem.getOwner() + "." + fieldItem.getFieldName()));

        // 获取当前表关联条件字段
        Map<FieldItem, List<FieldItem>> relationTableFields = tableItem.getRelationTableFields();
        relationTableFields.keySet()
            .forEach(fieldItem -> Assert.assertEquals("user_id", fieldItem.getColumn().getColumnName()));

        // 获取关联字段在select中的对应字段
        // List<FieldItem> relationSelectFieldItem =
        // tableItem.getRelationKeyFieldItems();
        // relationSelectFieldItem.forEach(fieldItem -> Assert.assertEquals("c.labels",
        // fieldItem.getOwner() + "." + fieldItem.getColumn().getColumnName()));
    }

    @Test
    public void visitGroupByAddVariantRefExprTest(){
        String sql = "select a.id, CASE WHEN a.id <= 500 THEN '1' else '2' end as id2, "
                + "concat(a.name,'_test') as name, a.role_id, b.name as role_name, c.labels from user a "
                + "left join role b on a.role_id=b.id "
                + "left join (select user_id, group_concat(label,',') as labels from user_label "
                + "group by user_id) c on c.user_id=a.id";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();

        SQLSelectQuery query = statement.getSelect().getQuery();
        SQLTableSource from = sqlSelectQueryBlock.getFrom();
        SQLJoinTableSource sqlJoinTableSource = ((SQLJoinTableSource) from);
        SqlParser.visitGroupByAddVariantRefExpr(sqlJoinTableSource);
        System.out.println(query.toString());
    }
}

package com.alibaba.otter.canal.client.adapter.es.test;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.es.config.SqlParser;

public class SqlParseTest {

    @Test
    public void parseTest() {
        // String sql = "select a.id,d.user_id2, concat(name,'_', a.nick) as name,
        // b.name as roleNme, d.name as typeName,concat(d.label,'_') as label,"
        // + " c.name as refName from user a left join role b on b.user_id=a.id "
        // + "left join type d on d.user_id=a.id and d.user_id2=a.p_id "
        // + "left join ( select ref_id,group_concat(name,',') as name from role group
        // by ref_id ) c on c.ref_id=a.id "
        // + "where a.id=1";
        String sql = "select a.id, concat(a.name,'_test') as name, a.role_id, b.name as role_name, c.labels from user a "
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
        List<FieldItem> fieldItems = schemaItem.getColumnFields().get(tableItem.getAlias() + ".label".toLowerCase());
        fieldItems.forEach(
            fieldItem -> Assert.assertEquals("c.labels", fieldItem.getOwner() + "." + fieldItem.getFieldName()));

        // 获取当前表关联条件字段
        List<FieldItem> relationTableFields = tableItem.getRelationTableFields();
        relationTableFields.forEach(fieldItem -> Assert.assertEquals("user_id", fieldItem.getColumn().getColumnName()));

        // 获取关联字段在select中的对应字段
        List<FieldItem> relationSelectFieldItem = tableItem.getRelationSelectFields();
        relationSelectFieldItem.forEach(fieldItem -> Assert.assertEquals("c.labels",
            fieldItem.getOwner() + "." + fieldItem.getColumn().getColumnName()));
    }
}

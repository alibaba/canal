package com.alibaba.otter.canal.adapter.es.core.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.adapter.es.core.parser.SqlParser;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.SqlItem;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.SqlItem.ColumnItem;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.SqlItem.FieldItem;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.SqlItem.SubQueryItem;
import com.alibaba.otter.canal.adapter.es.core.parser.sql.SqlItem.TableItem;

public class SqlParserTest {

    private Connection conn;

    @Before
    public void init() throws ClassNotFoundException, SQLException {
    }

    @After
    public void destroy() throws SQLException {
    }

    @Test
    public void test01() {
        String sql = "select id as _id,name,sex from user ";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        Assert.assertEquals(sqlItem.getTables().get("").getTableName(), "user");
        FieldItem fieldItem1 = sqlItem.getSelectFields().get("_id");
        Assert.assertTrue(fieldItem1.isRaw());
        Assert.assertTrue(fieldItem1.isSimple());
        Assert.assertEquals(fieldItem1.getColumnItems().iterator().next().getColumnName(), "id");
    }

    @Test
    public void test02() {
        String sql = "select t.id as _id, t.name, t.sex from user t ";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        TableItem tableItem1 = sqlItem.getTables().get("t");
        Assert.assertEquals(tableItem1.getTableName(), "user");
        Assert.assertEquals(tableItem1.getAlias(), "t");
        FieldItem fieldItem1 = sqlItem.getSelectFields().get("_id");
        Assert.assertTrue(fieldItem1.isRaw());
        Assert.assertTrue(fieldItem1.isSimple());
        Assert.assertEquals(fieldItem1.getOwners().iterator().next(), "t");
        Assert.assertEquals(fieldItem1.getColumnItems().iterator().next().getColumnName(), "id");
    }

    @Test
    public void test03() {
        String sql = "select t.id as _id, concat(t.name,'_',t.id) as name, t.sex from user t ";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        FieldItem fieldItem1 = sqlItem.getSelectFields().get("name");
        Assert.assertFalse(fieldItem1.isRaw());
        Assert.assertTrue(fieldItem1.isSimple());
        Assert.assertEquals(fieldItem1.getOwners().iterator().next(), "t");
        Set<ColumnItem> columnItems = fieldItem1.getColumnItems();
        Iterator<ColumnItem> columnItemIterator = columnItems.iterator();
        Assert.assertEquals(columnItemIterator.next().getColumnName(), "name");
        Assert.assertEquals(columnItemIterator.next().getColumnName(), "id");
    }

    @Test
    public void test04() {
        String sql = "select t.id as _id, t.name, t.sex, r.name as role_name from user t left join role r on t.role_id=r.id";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        TableItem tableItem1 = sqlItem.getTables().get("t");
        Assert.assertEquals(tableItem1.getTableName(), "user");
        Assert.assertEquals(tableItem1.getAlias(), "t");
        TableItem tableItem2 = sqlItem.getTables().get("r");
        Assert.assertEquals(tableItem2.getTableName(), "role");
        Assert.assertEquals(tableItem2.getAlias(), "r");

        FieldItem fieldItem1 = sqlItem.getSelectFields().get("role_name");
        Assert.assertTrue(fieldItem1.isRaw());
        Assert.assertTrue(fieldItem1.isSimple());
        ColumnItem columnItem = fieldItem1.getColumnItems().iterator().next();
        Assert.assertEquals(columnItem.getColumnName(), "name");
        Assert.assertEquals(columnItem.getOwner(), "r");
    }

    @Test
    public void test05() {
        String sql = "select t.id as _id, t.name, t.sex, l.labels from user t left join (select user_id, "
                     + "group_concat(label) as labels from user_label group by user_id) l on t.id=r.user_id";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        TableItem tableItem1 = sqlItem.getTables().get("t");
        Assert.assertEquals(tableItem1.getTableName(), "user");
        Assert.assertEquals(tableItem1.getAlias(), "t");

        SubQueryItem subQueryItem = (SubQueryItem) sqlItem.getTables().get("l");
        Assert.assertEquals(subQueryItem.getTableName(), "user_label");
        Assert.assertEquals(subQueryItem.getAlias(), "l");
        Assert.assertNotNull(subQueryItem.getGroupByItem());

        FieldItem fieldItem1 = sqlItem.getSelectFields().get("labels");
        Assert.assertTrue(fieldItem1.isRaw());
        Assert.assertTrue(fieldItem1.isSimple());
        ColumnItem columnItem = fieldItem1.getColumnItems().iterator().next();
        Assert.assertEquals(columnItem.getColumnName(), "labels");
        Assert.assertEquals(columnItem.getOwner(), "l");
    }

    /**
     * 测试带引号sql
     */
    @Test
    public void test06() {
        String sql = "select `id` as _id, `name`, `type` from `user` ";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        Assert.assertEquals(sqlItem.getTables().get("").getTableName(), "user");
        FieldItem fieldItem1 = sqlItem.getSelectFields().get("_id");
        Assert.assertTrue(fieldItem1.isRaw());
        Assert.assertTrue(fieldItem1.isSimple());
        Assert.assertEquals(fieldItem1.getColumnItems().iterator().next().getColumnName(), "id");
    }

    @Test
    public void test07() {
        String sql = "select id from user where deleted=0 ";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        Assert.assertTrue(sqlItem.getWhereItem().isSimple());
    }

    @Test
    public void test08() {
        String sql = "select a.id from user a left join role b on b.id=a.role_id where a.deleted=0 and b.deleted=0";
        SqlItem sqlItem = SqlParser.parse(conn, sql);
        Assert.assertFalse(sqlItem.getWhereItem().isSimple());
    }
}

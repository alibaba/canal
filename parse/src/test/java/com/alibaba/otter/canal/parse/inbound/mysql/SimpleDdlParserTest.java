package com.alibaba.otter.canal.parse.inbound.mysql;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.SimpleDdlParser;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.SimpleDdlParser.DdlResult;

public class SimpleDdlParserTest {

    @Test
    public void testCreate() {
        String queryString = "CREATE TABLE retl_mark ( `ID` int(11)";
        DdlResult result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE IF NOT EXIST retl.retl_mark ( `ID` int(11)";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE IF NOT EXIST `retl_mark` ( `ID` int(11)";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE IF NOT EXIST `retl.retl_mark` ( `ID` int(11)";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "CREATE TABLE  `retl`.`retl_mark` (\n  `ID` int(10) unsigned NOT NULL";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());
        
        queryString = "CREATE TABLE  `retl`.`retl_mark`(\n  `ID` int(10) unsigned NOT NULL";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());
    }

    @Test
    public void testDrop() {
        String queryString = "DROP TABLE retl_mark";
        DdlResult result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "DROP TABLE IF EXIST retl.retl_mark;";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "DROP TABLE IF EXIST \n `retl.retl_mark` ;";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());
    }

    @Test
    public void testAlert() {
        String queryString = "alter table retl_mark drop index emp_name";
        DdlResult result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "alter table retl.retl_mark drop index emp_name";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());

        queryString = "alter table \n `retl.retl_mark` drop index emp_name;";
        result = SimpleDdlParser.parse(queryString, "retl");
        Assert.assertNotNull(result);
        Assert.assertEquals("retl_mark", result.getTableName());
    }
}

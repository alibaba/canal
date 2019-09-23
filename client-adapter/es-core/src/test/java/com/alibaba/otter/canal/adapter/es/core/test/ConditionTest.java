package com.alibaba.otter.canal.adapter.es.core.test;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.otter.canal.adapter.es.core.parser.sql.Condition;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;

public class ConditionTest {
    @Test
    public void test01() {
        String sql = "select * from user where id=1";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("id", 1);
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test02() {
        String sql = "select * from user where name='Eric'";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("name", "Eric");
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test03() {
        String sql = "select * from user where c_time='2019-05-05 16:52:34'";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("c_time", DateTime.parse("2019-05-05T16:52:34+08").toDate());
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test04() {
        String sql = "select * from user where c_time='2019-05-05 16:52:34' or name='Eric'";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("c_time", DateTime.parse("2019-05-05T16:52:34+08").toDate());
        args.put("name", "Alex");
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test05() {
        String sql = "select * from user where c_time='2019-05-05 16:52:34' and (name='Eric' or id=1)";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("c_time", DateTime.parse("2019-05-05T16:52:34+08").toDate());
        args.put("name", "Alex");
        args.put("id", 1);
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test06() {
        String sql = "select * from user where id>10";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("id", 12);
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test07() {
        String sql = "select * from user where c_time>'2019-05-05'";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("c_time", DateTime.parse("2019-05-06").toDate());
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test08() {
        String sql = "select * from user where name>'abcd'";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("name", "bcde");
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test09() {
        String sql = "select * from user where id<>1";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("id", 2);
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test10() {
        String sql = "select * from user where name!='Eric'";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("name", "Alex");
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test11() {
        String sql = "select * from user where c_time!='2019-05-05 16:52:34'";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("c_time", DateTime.parse("2019-05-05T17:52:34+08").toDate());
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test12() {
        String sql = "select * from user where id is not null";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("id", 1);
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }

    @Test
    public void test13() {
        String sql = "select * from user where `type`=2";
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
        Map<String, Object> args = new HashMap<>();
        args.put("type", 2);
        boolean res = Condition.compareValue(sqlSelectQueryBlock.getWhere(), args);
        Assert.assertTrue(res);
    }
}

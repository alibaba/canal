package com.alibaba.otter.canal.adapter.es.core.test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.otter.canal.adapter.es.core.parser.sql.Function;
import com.alibaba.otter.canal.adapter.es.core.util.Util;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;

public class FunctionTest {
    @Test
    public void binaryOp() {
        {
            String sql = "select 10*23 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigInteger("230"));
        }
        {
            String sql = "select 10*23.56 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("235.60"));
        }
        {
            String sql = "select 23.56*10 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("235.60"));
        }
        {
            String sql = "select 23.56*1.1 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("25.916"));
        }
        {
            String sql = "select 10/6 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("10").divide(new BigDecimal("6"), MathContext.DECIMAL32));
        }
        {
            String sql = "select 10%6 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigInteger("4"));
        }
        {
            String sql = "select 20%6.1 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("1.7"));
        }
        {
            String sql = "select 10*4-7/8 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("39.125"));
        }
        {
            String sql = "select 10*(7-4)/8 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("3.75"));
        }
        {
            String sql = "select 10*(a.price-4)/8 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("a.price", 7);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, new BigDecimal("3.75"));
        }
        {
            String sql = "select length(10*(a.price-4)/8) ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("a.price", 7);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, 4);
        }
        {
            String sql = "select truncate(1314.1314, 3) ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 1314.131d);
        }
    }

    @Test
    public void caseWhen() {
        {
            String sql = "select CASE sex WHEN 1 THEN '男'  WHEN 2 THEN '女' ELSE '其他' END from user";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("sex", 1);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, "男");
        }
        {
            String sql = "select CASE WHEN sex = 1 THEN '男'  WHEN sex = 2 THEN '女' ELSE '其他' END  from user";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("sex", 2);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, "女");
        }
    }

    @Test
    public void mathOp() {
        {
            String sql = "select abs(-110) ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigInteger("110"));
        }
        {
            String sql = "select abs(-1341235356786345769234537585235) ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigInteger("1341235356786345769234537585235"));
        }
        {
            String sql = "select abs(-11.111) ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("11.111"));
        }
        {
            String sql = "select abs(price) from order ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("price", -10.2);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, new BigDecimal("10.2"));
        }
        {
            String sql = "select abs(a.price) from order a";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("a.price", -10.2);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, new BigDecimal("10.2"));
        }
        {
            // 嵌套测试
            String sql = "select abs(abs(a.price)) from order a";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("a.price", -10.2);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, new BigDecimal("10.2"));
        }
        {
            String sql = "select acos(0.2)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 1.369438406004566D);
        }
        {
            String sql = "select asin(0.2)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 0.2013579207903308D);
        }
        {
            String sql = "select atan2(0.2,0.3)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 0.5880026035475676D);
        }
        {
            String sql = "select ceiling(1.2)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 2d);
        }
        {
            String sql = "select cot(1)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 0.6420926159343306d);
        }
        {
            String sql = "select log(1.2)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 0.1823215567939546d);
        }
        {
            String sql = "select round(1.234)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 1d);
        }
        {
            String sql = "select round(1.234, 2)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 1.23d);
        }
        {
            String sql = "select sign(-123)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, -1);
        }
        {
            String sql = "select mod(10, 3.1)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, new BigDecimal("0.7"));
        }
        {
            String sql = "select ascii('a')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 97);
        }
        {
            String sql = "select bit_length('asdf')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 32);
        }
        {
            String sql = "select char(97)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 'a');
        }
        {
            String sql = "select instr('asdf','s')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 2);
        }
        {
            String sql = "select lcase('ASDF')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "asdf");
        }
        {
            String sql = "select left('asdf',3)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "asd");
        }
        {
            String sql = "select locate('s','asdsf')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 2);
        }
        {
            String sql = "select locate('s','asdsf',3)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 4);
        }
        {
            String sql = "select ltrim('  asdsf')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "asdsf");
        }
        {
            String sql = "select repeat('a',3)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "aaa");
        }
        {
            String sql = "select replace('asdsf','s','S')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "aSdSf");
        }
        {
            String sql = "select right('asdf',3)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "sdf");
        }
        {
            String sql = "select rtrim('asdsf  ')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "asdsf");
        }
        {
            String sql = "select space(4)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "    ");
        }
        {
            String sql = "select substr('asdf',2)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "sdf");
        }
        {
            String sql = "select substr('asdfg',2,3)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "sdf");
        }
        {
            String sql = "select ucase('asdf')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "ASDF");
        }
        {
            String sql = "select position('s' IN 'asdf')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 2);
        }
        {
            String sql = "select trim(' asdf  ')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "asdf");
        }
        {
            String sql = "select rpad('hi',4,'xxx')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "hixx");
        }
        {
            String sql = "select lpad('hi',4,'xxx')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "xxhi");
        }
        {
            String sql = "select to_char(12321)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "12321");
        }
        {
            String sql = "select concat(id,'_') from user";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("id", 7);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, "7_");
        }
        {
            String sql = "select concat_ws('_',id,'t') from user";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("id", 7);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, "7_t");
        }
    }

    @Test
    public void dateOp() {
        {
            String sql = "select DATE_ADD('2019-09-01 13:34:38',INTERVAL 2 YEAR)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, Util.parseDate("2021-09-01 13:34:38"));
        }
        {
            String sql = "select DATE_ADD('2019-09-01 13:34:38',INTERVAL 2 MONTH)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, Util.parseDate("2019-11-01 13:34:38"));
        }
        {
            String sql = "select DATE_ADD(a.c_time,INTERVAL 2 MONTH) from user a";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("a.c_time", DateTime.parse("2019-09-01T13:34:38+08").toDate());
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, Util.parseDate("2019-11-01 13:34:38"));
        }
        {
            String sql = "select DATEDIFF('2019-11-08','2019-09-01')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 68);
        }
        {
            String sql = "select DAYNAME('2019-09-01 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "Sunday");
        }
        {
            String sql = "select DAYOFMONTH('2019-09-12 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 12);
        }
        {
            String sql = "select DAYOFWEEK('2019-09-10 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 3);
        }
        {
            String sql = "select DAYOFYEAR('2019-09-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 251);
        }
        {
            String sql = "select HOUR('2019-09-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 23);
        }
        {
            String sql = "select MINUTE('2019-09-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 52);
        }
        {
            String sql = "select MONTH('2019-09-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 9);
        }
        {
            String sql = "select MONTHNAME('2019-05-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, "May");
        }
        {
            String sql = "select QUARTER('2019-07-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 3);
        }
        {
            String sql = "select SECOND('2019-07-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 17);
        }
        {
            String sql = "select WEEK('2019-09-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 36);
        }
        {
            String sql = "select YEAR('2019-09-08 23:52:17')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Object res = Function.getValue(si.getExpr(), null);
            Assert.assertEquals(res, 2019);
        }
    }

    @Test
    public void commonOp() {
        {
            String sql = "select ifnull(sex, '其他')";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("sex", null);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, "其他");
        }
        {
            String sql = "select nullif(sex, 2)";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("sex", (byte) 1);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, (byte) 1);
        }
        {
            String sql = "select nullif(c_time,'2019-04-17 13:48:20') from user  where id=1";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("c_time", new Timestamp(DateTime.parse("2019-04-17T13:48:20+08").toDate().getTime()));
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertNull(res);
        }
        {
            String sql = "select isnull(c_time) from user  where id=1";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("c_time", null);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, (byte) 1);
        }
    }

    @Test
    public void test01() {
        {
            String sql = "select 10*(a.`type`-4)/8 ";
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
            MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();
            SQLSelectItem si = sqlSelectQueryBlock.getSelectList().get(0);
            Map<String, Object> args = new HashMap<>();
            args.put("a.type", 7);
            Object res = Function.getValue(si.getExpr(), args);
            Assert.assertEquals(res, new BigDecimal("3.75"));
        }
    }
}

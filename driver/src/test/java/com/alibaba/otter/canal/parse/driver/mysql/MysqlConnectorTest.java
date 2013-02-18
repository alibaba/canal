package com.alibaba.otter.canal.parse.driver.mysql;

import java.io.IOException;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;

public class MysqlConnectorTest {

    @Test
    public void testQuery() {

        MysqlConnector connector = new MysqlConnector(new InetSocketAddress("10.20.144.15", 3306), "ottermysql",
                                                      "ottermysql");
        try {
            connector.connect();
            MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
            ResultSetPacket result = exector.query("desc test.lj_table1");
            System.out.println(result);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        } finally {
            try {
                connector.disconnect();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void testUpdate() {

        MysqlConnector connector = new MysqlConnector(new InetSocketAddress("10.20.144.15", 3306), "ottermysql",
                                                      "ottermysql");
        try {
            connector.connect();
            MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
            exector.update("update otter1.otter_stability1 set timestamp_values = now() where id < 5000010");
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        } finally {
            try {
                connector.disconnect();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }
    }
}

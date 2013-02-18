package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.server.ResultSetPacket;

public class MysqlComunicationest {

    @Test
    public void testQuery() {

        MysqlConnection connection = new MysqlConnection(11L, new InetSocketAddress("10.20.144.15", 3306),
                                                         "ottermysql", "ottermysql");
        try {
            connection.connect();
            ResultSetPacket result = connection.query("desc test.lj_table1");
            System.out.println(result);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        } finally {
            try {
                connection.disconnect();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void testUpdate() {

        MysqlConnection connection = new MysqlConnection(11L, new InetSocketAddress("10.20.144.15", 3306),
                                                         "ottermysql", "ottermysql");
        try {
            connection.connect();
            connection.update("update otter1.otter_stability1 set timestamp_values = now() where id < 5000010");
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        } finally {
            try {
                connection.disconnect();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }
    }
}

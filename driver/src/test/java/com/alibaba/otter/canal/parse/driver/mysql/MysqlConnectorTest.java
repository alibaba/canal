package com.alibaba.otter.canal.parse.driver.mysql;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;

public class MysqlConnectorTest {

    public static void main(String args[]) {
        MysqlConnector connector = new MysqlConnector(new InetSocketAddress("yddb01.mysql.database.chinacloudapi.cn",
            3306), "ps-admin01@yddb01", "1qaz3edc");
        try {
            connector.connect();
            MysqlQueryExecutor executor = new MysqlQueryExecutor(connector);
            ResultSetPacket result = executor.query("show variables like '%char%';");
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connector.disconnect();
            } catch (IOException e) {
            }
        }
    }

}

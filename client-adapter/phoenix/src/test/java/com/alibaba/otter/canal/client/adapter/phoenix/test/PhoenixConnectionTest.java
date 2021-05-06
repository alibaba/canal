package com.alibaba.otter.canal.client.adapter.phoenix.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @author: lihua
 * @date: 2021/1/5 16:58
 * @Description:
 */
public class PhoenixConnectionTest {

    public static void main(String[] args) {
        Properties phoenixPro = new Properties();
        //phoenix内部本身有连接池，不需要使用Druid初始化
        phoenixPro.setProperty("hbase.rpc.timeout","600000");
        phoenixPro.setProperty("hbase.client.scanner.timeout.period","600000");
        phoenixPro.setProperty("dfs.client.socket-timeout","600000");
        phoenixPro.setProperty("phoenix.query.keepAliveMs","600000");
        phoenixPro.setProperty("phoenix.query.timeoutMs","3600000");
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Connection connection = DriverManager.getConnection("jdbc:phoenix:zookeeper01,zookeeper02,zookeeper03:2181:/hbase/db", phoenixPro);
            System.out.println(connection);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

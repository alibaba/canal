package com.alibaba.otter.canal.client.adapter.es6x.test;

import java.sql.SQLException;

import com.alibaba.druid.pool.DruidDataSource;

public class TestConstant {

    public final static String    jdbcUrl      = "jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true";
    public final static String    jdbcUser     = "root";
    public final static String    jdbcPassword = "121212";

    public final static String    esHosts      = "127.0.0.1:9300";
    public final static String    clusterName  = "elasticsearch";

    public final static DruidDataSource dataSource;

    static {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(jdbcUser);
        dataSource.setPassword(jdbcPassword);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(1);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
        dataSource.setValidationQuery("select 1");
        try {
            dataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

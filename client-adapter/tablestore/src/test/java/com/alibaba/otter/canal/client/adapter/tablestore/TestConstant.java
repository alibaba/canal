package com.alibaba.otter.canal.client.adapter.tablestore;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.SQLException;

public class TestConstant {

    /**
     * db配置
     */
    public final static String    jdbcUrl      = "jdbc:mysql://xxxxxxxxx";
    public final static String    jdbcUser     = "xxxxxx";
    public final static String    jdbcPassword = "xxxxxx";

    public final static DruidDataSource dataSource;

    static {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
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

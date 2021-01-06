package com.alibaba.otter.canal.client.adapter.phoenix.test;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.SQLException;

/**
 * @author: lihua
 * @date: 2021/1/5 17:09
 * @Description:
 */
public class TestConstant {
    public final static String          jdbcUrl      = "jdbc:mysql://mysql.yanfa.center:3306/BUSINES_LOG?useUnicode=true";
    public final static String          jdbcUser     = "chensz";
    public final static String          jdbcPassword = "chensz";

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

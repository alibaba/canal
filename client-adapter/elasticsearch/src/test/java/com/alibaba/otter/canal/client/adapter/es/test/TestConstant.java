package com.alibaba.otter.canal.client.adapter.es.test;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.SQLException;

public class TestConstant {

    public final static String    jdbcUrl      = "jdbc:mysql://192.168.81.48:3306/mytest?useUnicode=true";
    public final static String    jdbcUser     = "root";
    public final static String    jdbcPassword = "root";

    public final static String    esHosts      = "192.168.81.48:9300";
    public final static String    esHttpHosts      = "192.168.81.48:9200";
    public final static String    nameAndPwd = "elastic:elastic";
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
        dataSource.setMaxActive(3);
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

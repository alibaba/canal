package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.vendor.MySqlExceptionSorter;
import com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker;
import com.alibaba.otter.canal.parse.exception.CanalParseException;

/**
 * Created by wanshao Date: 2017/9/22 Time: 下午2:46
 **/
public class DataSourceFactoryTSDB {

    public static DataSource getDataSource(String url, String userName, String password) {
        try {
            DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setUrl(url);
            druidDataSource.setUsername(userName);
            druidDataSource.setPassword(password);
            druidDataSource.setTestWhileIdle(true);
            druidDataSource.setTestOnBorrow(false);
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setNotFullTimeoutRetryCount(2);
            druidDataSource.setValidConnectionCheckerClassName(MySqlValidConnectionChecker.class.getName());
            druidDataSource.setExceptionSorterClassName(MySqlExceptionSorter.class.getName());
            druidDataSource.setValidationQuery("SELECT 1");
            druidDataSource.setInitialSize(1);
            druidDataSource.setMinIdle(1);
            druidDataSource.setMaxActive(30);
            druidDataSource.setMaxWait(10 * 1000);
            druidDataSource.setTimeBetweenEvictionRunsMillis(60 * 1000);
            druidDataSource.setMinEvictableIdleTimeMillis(50 * 1000);
            druidDataSource.setUseUnfairLock(true);
            druidDataSource.init();
            return druidDataSource;
        } catch (Throwable e) {
            throw new CanalParseException("init druidDataSource failed", e);
        }
    }
}

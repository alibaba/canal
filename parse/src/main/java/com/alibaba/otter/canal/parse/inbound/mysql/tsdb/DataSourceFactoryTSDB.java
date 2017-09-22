package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.parse.exception.CanalParseException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wanshao
 * Date: 2017/9/22
 * Time: 下午2:46
 **/
public class DataSourceFactoryTSDB {

    private static Logger logger = LoggerFactory.getLogger(DataSourceFactoryTSDB.class);

    public static DataSource getDataSource(String address, String userName, String password,
                                           boolean enable,
                                           String defaultDatabaseName, String url, String driverClassName) {

        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(userName);
        druidDataSource.setPassword(password);

        try {
            druidDataSource.init();
        } catch (SQLException e) {
            logger.error("druidDataSource.init", e);
            throw new CanalParseException("初始化druid dataSource出错");
        }
        return druidDataSource;
    }


}

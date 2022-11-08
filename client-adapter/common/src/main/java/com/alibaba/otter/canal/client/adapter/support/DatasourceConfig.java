package com.alibaba.otter.canal.client.adapter.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * 数据源配置
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
public class DatasourceConfig {

    public final static Map<String, DruidDataSource> DATA_SOURCES = new ConcurrentHashMap<>(); // key对应的数据源

    private String                                   driver       = "com.mysql.jdbc.Driver";   // 默认为mysql jdbc驱动
    private String                                   url;                                      // jdbc url
    private String                                   database;                                 // jdbc database
    private String                                   type         = "mysql";                   // 类型, 默认为mysql
    private String                                   username;                                 // jdbc username
    private String                                   password;                                 // jdbc password
    private Integer                                  maxActive    = 3;                         // 连接池最大连接数,默认为3

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(Integer maxActive) {
        this.maxActive = maxActive;
    }
}

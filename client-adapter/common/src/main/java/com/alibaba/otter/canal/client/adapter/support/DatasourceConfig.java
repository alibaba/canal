package com.alibaba.otter.canal.client.adapter.support;

import com.alibaba.druid.pool.DruidDataSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatasourceConfig {

    public final static Map<String, DruidDataSource> DATA_SOURCES = new ConcurrentHashMap<>();

    private String                                   driver       = "com.mysql.jdbc.Driver";
    private String                                   url;
    private String                                   database;
    private String                                   type         = "mysql";
    private String                                   username;
    private String                                   password;
    private Integer                                  maxActive    = 3;

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

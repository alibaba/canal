package com.alibaba.otter.canal.client.adapter.starrocks.support;

import com.alibaba.otter.canal.client.adapter.starrocks.config.MappingConfig;

import java.util.List;

public class StarRocksBufferData {
    private String dbName;

    private MappingConfig mappingConfig;
    private List<byte[]> data;
    public StarRocksBufferData() {

    }
    public StarRocksBufferData(String database, String tableName, List<byte[]> data) {
        this.dbName = database;
        this.data  = data;
    }
    public String getDbName() {
        return dbName;
    }
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    public List<byte[]> getData() {
        return data;
    }

    public void setData(List<byte[]> data) {
        this.data = data;
    }

    public MappingConfig getMappingConfig() {
        return mappingConfig;
    }

    public void setMappingConfig(MappingConfig mappingConfig) {
        this.mappingConfig = mappingConfig;
    }
}

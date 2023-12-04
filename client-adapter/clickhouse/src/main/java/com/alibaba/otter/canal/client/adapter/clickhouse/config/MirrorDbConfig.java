package com.alibaba.otter.canal.client.adapter.clickhouse.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: Xander
 * @date: Created in 2023/11/10 22:23
 * @email: zhrunxin33@gmail.com
 * @version 1.1.8
 */

public class MirrorDbConfig {

    private String                     fileName;
    private MappingConfig              mappingConfig;
    private Map<String, MappingConfig> tableConfig = new ConcurrentHashMap<>();

    public static MirrorDbConfig create(String fileName, MappingConfig mappingConfig) {
        return new MirrorDbConfig(fileName, mappingConfig);
    }

    public MirrorDbConfig(String fileName, MappingConfig mappingConfig){
        this.fileName = fileName;
        this.mappingConfig = mappingConfig;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public MappingConfig getMappingConfig() {
        return mappingConfig;
    }

    public void setMappingConfig(MappingConfig mappingConfig) {
        this.mappingConfig = mappingConfig;
    }

    public Map<String, MappingConfig> getTableConfig() {
        return tableConfig;
    }

    public void setTableConfig(Map<String, MappingConfig> tableConfig) {
        this.tableConfig = tableConfig;
    }
}

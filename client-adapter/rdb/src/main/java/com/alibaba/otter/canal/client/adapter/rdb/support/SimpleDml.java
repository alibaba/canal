package com.alibaba.otter.canal.client.adapter.rdb.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.apache.commons.lang.StringUtils;

public class SimpleDml {

    private String              destination;
    private String              database;
    private String              table;
    private String              type;
    private Map<String, Object> data;
    private Map<String, Object> old;

    private MappingConfig       config;

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Map<String, Object> getOld() {
        return old;
    }

    public void setOld(Map<String, Object> old) {
        this.old = old;
    }

    public MappingConfig getConfig() {
        return config;
    }

    public void setConfig(MappingConfig config) {
        this.config = config;
    }

    public static List<SimpleDml> dml2SimpleDml(Dml dml, MappingConfig config) {
        List<SimpleDml> simpleDmlList = new ArrayList<>();
        int len = dml.getData().size();

        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String database = dml.getDatabase();
        String table = dml.getTable();

        for (int i = 0; i < len; i++) {
            SimpleDml simpleDml = new SimpleDml();
            simpleDml.setDestination(dml.getDestination());
            simpleDml.setDatabase(dml.getDatabase());
            simpleDml.setTable(dml.getTable());
            simpleDml.setType(dml.getType());
            simpleDml.setData(dml.getData().get(i));
            if (dml.getOld() != null) {
                simpleDml.setOld(dml.getOld().get(i));
            }
            simpleDml.setConfig(config);
            simpleDmlList.add(simpleDml);
        }

        return simpleDmlList;
    }
}

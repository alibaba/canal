package com.alibaba.otter.canal.client.adapter.rdb.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.util.LinkedCaseInsensitiveMap;

import com.alibaba.otter.canal.client.adapter.support.Dml;

public class SingleDml {

    private String              destination;
    private String              database;
    private String              table;
    private String              type;
    private Map<String, Object> data;
    private Map<String, Object> old;

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

    public static List<SingleDml> dml2SingleDmls(Dml dml, boolean caseInsensitive) {
        List<SingleDml> singleDmls = new ArrayList<>();
        if (dml.getData() != null) {
            int size = dml.getData().size();
            for (int i = 0; i < size; i++) {
                SingleDml singleDml = new SingleDml();
                singleDml.setDestination(dml.getDestination());
                singleDml.setDatabase(dml.getDatabase());
                singleDml.setTable(dml.getTable());
                singleDml.setType(dml.getType());
                Map<String, Object> data = dml.getData().get(i);
                if (caseInsensitive) {
                    data = toCaseInsensitiveMap(data);
                }
                singleDml.setData(data);
                if (dml.getOld() != null) {
                    Map<String, Object> oldData = dml.getOld().get(i);
                    if (caseInsensitive) {
                        oldData = toCaseInsensitiveMap(oldData);
                    }
                    singleDml.setOld(oldData);
                }
                singleDmls.add(singleDml);
            }
        } else if ("TRUNCATE".equalsIgnoreCase(dml.getType())) {
            SingleDml singleDml = new SingleDml();
            singleDml.setDestination(dml.getDestination());
            singleDml.setDatabase(dml.getDatabase());
            singleDml.setTable(dml.getTable());
            singleDml.setType(dml.getType());
            singleDmls.add(singleDml);
        }
        return singleDmls;
    }

    public static List<SingleDml> dml2SingleDmls(Dml dml) {
        return dml2SingleDmls(dml, false);
    }

    private static <V> LinkedCaseInsensitiveMap<V> toCaseInsensitiveMap(Map<String, V> data) {
        LinkedCaseInsensitiveMap map = new LinkedCaseInsensitiveMap();
        map.putAll(data);
        return map;
    }
}

package com.alibaba.otter.canal.client.adapter.support;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AdapterConfigs {

    private static Map<String, Set<String>> configs = new ConcurrentHashMap<>();

    public static void put(String key, String value) {
        Set<String> values = configs.get(key);
        if (values == null) {
            values = new LinkedHashSet<>();
        }
        values.add(value);
        configs.put(key, values);
    }

    public static Set<String> get(String key) {
        return configs.get(key);
    }

    public static void clear() {
        configs.clear();
    }
}

package com.alibaba.otter.canal.client.adapter.support;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 适配器配置集合, 用于配置加载, 线程不安全
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
public class AdapterConfigs {

    /**
     * 类型下对应所有配置名, 如:
     * hbase
     *  ┗━ mytest_person.yml
     *  ┗━ mytest_role.yml
     *  ┗━ mytest_department.yml
     */
    private static Map<String, Set<String>> configs = new HashMap<>();

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

package com.alibaba.otter.canal.client.adapter.phoenix.test.sync;

import com.alibaba.otter.canal.client.adapter.phoenix.PhoenixAdapter;
import com.alibaba.otter.canal.client.adapter.phoenix.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: lihua
 * @date: 2021/1/5 23:15
 * @Description:
 */
public class Common {
    public static PhoenixAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("phoenix");
        outerAdapterConfig.setKey("phoenix");
        Map<String, String> properties = new HashMap<>();
        properties.put("jdbc.driverClassName", "org.apache.phoenix.jdbc.PhoenixDriver");
        properties.put("jdbc.url", "jdbc:phoenix:zookeeper01,zookeeper02,zookeeper03:2181:/hbase/db");
        outerAdapterConfig.setProperties(properties);

        PhoenixAdapter adapter = new PhoenixAdapter();
        adapter.init(outerAdapterConfig, null);
        return adapter;
    }

    public static void main(String[] args) {
        init();
    }
}

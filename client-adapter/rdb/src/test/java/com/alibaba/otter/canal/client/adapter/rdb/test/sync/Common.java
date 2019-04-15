package com.alibaba.otter.canal.client.adapter.rdb.test.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

public class Common {

    public static RdbAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("rdb");
        outerAdapterConfig.setKey("oracle1");
        Map<String, String> properties = new HashMap<>();
        properties.put("jdbc.driveClassName", "oracle.jdbc.OracleDriver");
        properties.put("jdbc.url", "jdbc:oracle:thin:@127.0.0.1:49161:XE");
        properties.put("jdbc.username", "mytest");
        properties.put("jdbc.password", "m121212");
        outerAdapterConfig.setProperties(properties);

        RdbAdapter adapter = new RdbAdapter();
        adapter.init(outerAdapterConfig, null);
        return adapter;
    }
}

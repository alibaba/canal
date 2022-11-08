package com.alibaba.otter.canal.client.adapter.kudu.test.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.kudu.KuduAdapter;
import com.alibaba.otter.canal.client.adapter.kudu.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

/**
 * @description
 */
public class Common {

    public static KuduAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("kudu");
        outerAdapterConfig.setKey("kudu");
        Map<String, String> properties = new HashMap<>();
        properties.put("kudu.master.address", "10.6.36.102,10.6.36.187,10.6.36.229");
        outerAdapterConfig.setProperties(properties);

        KuduAdapter adapter = new KuduAdapter();
        adapter.init(outerAdapterConfig, null);
        return adapter;
    }
}

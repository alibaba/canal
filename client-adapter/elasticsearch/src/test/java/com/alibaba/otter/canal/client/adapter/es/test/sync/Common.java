package com.alibaba.otter.canal.client.adapter.es.test.sync;

import com.alibaba.otter.canal.client.adapter.es.ESAdapter;
import com.alibaba.otter.canal.client.adapter.es.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

import java.util.HashMap;
import java.util.Map;

public class Common {

    public static ESAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("es");
        outerAdapterConfig.setHosts(TestConstant.esHosts);
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", TestConstant.clusterNmae);
        outerAdapterConfig.setProperties(properties);

        ESAdapter esAdapter = new ESAdapter();
        esAdapter.init(outerAdapterConfig);
        return esAdapter;
    }
}

package com.alibaba.otter.canal.client.adapter.es.test;

import java.util.*;

import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.es.ESAdapter;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

public class SyncTest {

    private ESAdapter esAdapter;

    @Before
    public void init() {
        AdapterConfigs.put("es", "mytest_user.yml");
        DatasourceConfig.DATA_SOURCES.put("defaultDS", DataSourceConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("es");
        outerAdapterConfig.setHosts("127.0.0.1:9300");
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", "elasticsearch");
        outerAdapterConfig.setProperties(properties);

        esAdapter = new ESAdapter();
        esAdapter.init(outerAdapterConfig);

    }

    @Test
    public void insertTest01() {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("INSERT");
        dml.setDatabase("mytest");
        dml.setTable("user");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 1L);
        data.put("name", "Eric");
        data.put("c_time", new Date());

        dml.setData(dataList);

        esAdapter.sync(dml);
    }
}

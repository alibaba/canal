package com.alibaba.otter.canal.client.adapter.es.test;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

public class ConfigLoadTest {

    @Before
    public void before() {
        AdapterConfigs.put("es", "mytest_user.yml");
        // 加载数据源连接池
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);
    }

    @Test
    public void testLoad() {
        ESSyncConfigLoader.load();
        Map<String, ESSyncConfig> configMap = ESSyncConfigLoader.getEsSyncConfig();
        ESSyncConfig config = configMap.get("mytest_user.yml");
        Assert.assertNotNull(config);
        Assert.assertEquals("defaultDS", config.getDataSourceKey());
        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
        Assert.assertEquals("mytest_user", esMapping.get_index());
        Assert.assertEquals("_doc", esMapping.get_type());
        Assert.assertEquals("id", esMapping.get_id());
        Assert.assertNotNull(esMapping.getSql());

        Map<String, List<ESSyncConfig>> dbTableEsSyncConfig = ESSyncConfigLoader.getDbTableEsSyncConfig();
        Assert.assertFalse(dbTableEsSyncConfig.isEmpty());
    }
}

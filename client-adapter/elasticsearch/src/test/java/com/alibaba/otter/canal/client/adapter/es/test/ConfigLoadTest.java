package com.alibaba.otter.canal.client.adapter.es.test;

import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class ConfigLoadTest {

    @Before
    public void before() {
        AdapterConfigs.put("es", "myetst_user.yml");
    }

    @Test
    public void testLoad() {
        Map<String, ESSyncConfig> configMap = ESSyncConfigLoader.load();
        ESSyncConfig config = configMap.get("myetst_user.yml");
        Assert.assertNotNull(config);
        Assert.assertEquals("defaultDS", config.getDataSourceKey());
        ESSyncConfig.ESMapping esMapping = config.getEsMapping();
        Assert.assertEquals("mytest_user", esMapping.get_index());
        Assert.assertEquals("_doc", esMapping.get_type());
        Assert.assertEquals("id", esMapping.get_id());
        Assert.assertNotNull(esMapping.getSql());
    }
}

package com.alibaba.otter.canal.client.adapter.rdb.test;

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

public class ConfigLoadTest {

    @Before
    public void before() {
        AdapterConfigs.put("rdb", "mytest_user.yml");
        // 加载数据源连接池
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);
    }

    @Test
    public void testLoad() {
        Map<String, MappingConfig> configMap =  MappingConfigLoader.load();

        Assert.assertFalse(configMap.isEmpty());
    }
}

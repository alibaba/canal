package com.alibaba.otter.canal.client.adapter.kudu.test;

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

/**
 * @description
 */
public class TestConfig {

    @Before
    public void before() {
        // 加载数据源连接池
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);
    }

    @Test
    public void testLoad() {
        Map<String, KuduMappingConfig> configMap = KuduMappingConfigLoader.load(null);
        Assert.assertFalse(configMap.isEmpty());
    }
}

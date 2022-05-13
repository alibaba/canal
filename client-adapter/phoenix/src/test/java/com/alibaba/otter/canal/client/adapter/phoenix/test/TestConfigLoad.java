package com.alibaba.otter.canal.client.adapter.phoenix.test;

import com.alibaba.otter.canal.client.adapter.phoenix.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.phoenix.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @author: lihua
 * @date: 2021/1/5 17:07
 * @Description:
 */
public class TestConfigLoad {
    @Before
    public void before() {
        // 加载数据源连接池
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);
    }

    @Test
    public void testLoad() {
        Map<String, MappingConfig> configMap = ConfigLoader.load(null);
        Assert.assertFalse(configMap.isEmpty());
    }
}

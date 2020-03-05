package com.alibaba.otter.canal.client.adapter.kudu.test;

import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfig;
import com.alibaba.otter.canal.client.adapter.kudu.config.KuduMappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　┻　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃  神兽保佑
 * 　　　　┃　　　┃  代码无bug
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * Created by Liuyadong on 2019-11-13
 *
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

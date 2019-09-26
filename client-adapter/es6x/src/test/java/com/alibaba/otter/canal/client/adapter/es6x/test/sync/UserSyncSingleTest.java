package com.alibaba.otter.canal.client.adapter.es6x.test.sync;

import java.util.*;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es6x.ES6xAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;

@Ignore
public class UserSyncSingleTest {

    private ES6xAdapter esAdapter;

    @Before
    public void init() {
        // AdapterConfigs.put("es", "mytest_user_single.yml");
        esAdapter = Common.init();
    }

    /**
     * 单表插入
     */
    @Test
    public void test01() {
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
        data.put("role_id", 1L);
        data.put("c_time", new Date());
        dml.setData(dataList);

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertEquals("Eric", response.getSource().get("_name"));
    }

    /**
     * 单表更新
     */
    @Test
    public void test02() {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("UPDATE");
        dml.setDatabase("mytest");
        dml.setTable("user");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 1L);
        data.put("name", "Eric2");
        dml.setData(dataList);
        List<Map<String, Object>> oldList = new ArrayList<>();
        Map<String, Object> old = new LinkedHashMap<>();
        oldList.add(old);
        old.put("name", "Eric");
        dml.setOld(oldList);

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertEquals("Eric2", response.getSource().get("_name"));
    }

    /**
     * 单表删除
     */
    @Test
    public void test03() {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("DELETE");
        dml.setDatabase("mytest");
        dml.setTable("user");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 1L);
        data.put("name", "Eric");
        data.put("role_id", 1L);
        data.put("c_time", new Date());
        dml.setData(dataList);

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertNull(response.getSource());
    }

    // @After
    // public void after() {
    // esAdapter.destroy();
    // DatasourceConfig.DATA_SOURCES.values().forEach(DruidDataSource::close);
    // }
}

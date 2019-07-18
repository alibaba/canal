package com.alibaba.otter.canal.client.adapter.es.test.sync;

import com.alibaba.otter.canal.client.adapter.es.ESHttpAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class HttpUserSyncSingleTest {

    private ESHttpAdapter esAdapter;

    @Before
    public void init() {
        // AdapterConfigs.put("es", "mytest_user_single.yml");
        esAdapter = CommonHttp.init();
    }

    /**
     * 单表插入
     */
    @Test
    public void test01() throws IOException {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("INSERT");
        dml.setDatabase("mytest");
        dml.setTable("user");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 2L);
        data.put("name", "Eric");
        data.put("role_id", 2L);
        data.put("c_time", new Date());
        dml.setData(dataList);

        String destination = dml.getDestination();
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(destination + "_" + database + "-" + table);

        esAdapter.getEsHttpSyncService().sync(esSyncConfigs.values(), dml);
        esAdapter.getEsHttpSyncService().commit();
        GetRequest request = new GetRequest("mytest_user", "_doc", "2");
        GetResponse response = esAdapter.getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
        Assert.assertEquals("Eric", response.getSource().get("_name"));
    }

    /**
     * 单表更新
     */
    @Test
    public void test02() throws IOException {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("UPDATE");
        dml.setDatabase("mytest");
        dml.setTable("user");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 2L);
        data.put("name", "Eric2");
        dml.setData(dataList);
        List<Map<String, Object>> oldList = new ArrayList<>();
        Map<String, Object> old = new LinkedHashMap<>();
        oldList.add(old);
        old.put("name", "Eric");
        dml.setOld(oldList);

        String destination = dml.getDestination();
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(destination + "_" + database + "-" + table);

        esAdapter.getEsHttpSyncService().sync(esSyncConfigs.values(), dml);
        esAdapter.getEsHttpSyncService().commit();
        GetRequest request = new GetRequest("mytest_user", "_doc", "2");
        GetResponse response = esAdapter.getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
        Assert.assertEquals("Eric2", response.getSource().get("_name"));
    }

    /**
     * 单表删除
     */
    @Test
    public void test03() throws IOException {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("DELETE");
        dml.setDatabase("mytest");
        dml.setTable("user");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 2L);
        data.put("name", "Eric");
        data.put("role_id", 2L);
        data.put("c_time", new Date());
        dml.setData(dataList);

        String destination = dml.getDestination();
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(destination + "_" + database + "-" + table);

        esAdapter.getEsHttpSyncService().sync(esSyncConfigs.values(), dml);
        esAdapter.getEsHttpSyncService().commit();
        GetRequest request = new GetRequest("mytest_user", "_doc", "2");
        GetResponse response = esAdapter.getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
        Assert.assertNull(response.getSource());
    }

    // @After
    // public void after() {
    // esAdapter.destroy();
    // DatasourceConfig.DATA_SOURCES.values().forEach(DruidDataSource::close);
    // }
}

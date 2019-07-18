package com.alibaba.otter.canal.client.adapter.es.test.sync;

import com.alibaba.otter.canal.client.adapter.es.ESHttpAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;

public class HttpUserSyncJoinOneTest {

    private ESHttpAdapter esAdapter;

    @Before
    public void init() {
        // AdapterConfigs.put("es", "mytest_user_join_one.yml");
        esAdapter = CommonHttp.init();
    }

    /**
     * 主表带函数插入
     */
    @Test
    public void test01() throws IOException {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds, "delete from user where id=1");
        Common.sqlExe(ds, "insert into user (id,name,role_id) values (1,'Eric',1)");

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

        String destination = dml.getDestination();
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(destination + "_" + database + "-" + table);
        esAdapter.getEsHttpSyncService().sync(esSyncConfigs.values(), dml);
        esAdapter.getEsHttpSyncService().commit();

        GetRequest request = new GetRequest("mytest_user", "_doc", "1");
        GetResponse response = esAdapter.getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
        Assert.assertEquals("Eric", response.getSource().get("_name"));
    }

    /**
     * 主表带函数更新
     */
    @Test
    public void test02() throws IOException {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds, "update user set name='Eric2' where id=1");

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

        String destination = dml.getDestination();
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(destination + "_" + database + "-" + table);
        esAdapter.getEsHttpSyncService().sync(esSyncConfigs.values(), dml);
        esAdapter.getEsHttpSyncService().commit();

        GetRequest request = new GetRequest("mytest_user", "_doc", "1");
        GetResponse response = esAdapter.getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
        Assert.assertEquals("Eric2", response.getSource().get("_name"));
    }
}

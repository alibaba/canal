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

public class HttpRoleSyncJoinOne2Test {

    private ESHttpAdapter esAdapter;

    @Before
    public void init() {
        // AdapterConfigs.put("es", "mytest_user_join_one2.yml");
        esAdapter = CommonHttp.init();
    }

    /**
     * 带函数非子查询从表插入
     */
    @Test
    public void test01() throws IOException {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds, "delete from role where id=1");
        Common.sqlExe(ds, "insert into role (id,role_name) values (1,'admin')");

        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("INSERT");
        dml.setDatabase("mytest");
        dml.setTable("role");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 1L);
        data.put("role_name", "admin");
        dml.setData(dataList);

        String destination = dml.getDestination();
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(destination + "_" + database + "-" + table);
        esAdapter.getEsHttpSyncService().sync(esSyncConfigs.values(), dml);
        esAdapter.getEsHttpSyncService().commit();


        GetRequest request = new GetRequest("mytest_user", "_doc", "1");
        GetResponse response = esAdapter.getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
        Assert.assertEquals("admin", response.getSource().get("_role_name"));
    }

    /**
     * 带函数非子查询从表更新
     */
    @Test
    public void test02() throws IOException {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds, "update role set role_name='admin3' where id=1");

        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("UPDATE");
        dml.setDatabase("mytest");
        dml.setTable("role");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 1L);
        data.put("role_name", "admin3");
        dml.setData(dataList);

        List<Map<String, Object>> oldList = new ArrayList<>();
        Map<String, Object> old = new LinkedHashMap<>();
        oldList.add(old);
        old.put("role_name", "admin");
        dml.setOld(oldList);

        String destination = dml.getDestination();
        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(destination + "_" + database + "-" + table);
        esAdapter.getEsHttpSyncService().sync(esSyncConfigs.values(), dml);
        esAdapter.getEsHttpSyncService().commit();

        GetRequest request = new GetRequest("mytest_user", "_doc", "1");
        GetResponse response = esAdapter.getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
        Assert.assertEquals("admin3", response.getSource().get("_role_name"));
    }
}

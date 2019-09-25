package com.alibaba.otter.canal.client.adapter.es6x.test.sync;

import java.util.*;

import javax.sql.DataSource;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es6x.ES6xAdapter;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

@Ignore
public class RoleSyncJoinOneTest {

    private ES6xAdapter esAdapter;

    @Before
    public void init() {
        // AdapterConfigs.put("es", "mytest_user_join_one.yml");
        esAdapter = Common.init();
    }

    /**
     * 非子查询从表插入
     */
    @Test
    public void test01() {
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

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertEquals("admin", response.getSource().get("_role_name"));
    }

    /**
     * 非子查询从表更新
     */
    @Test
    public void test02() {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds, "update role set role_name='admin2' where id=1");

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
        data.put("role_name", "admin2");
        dml.setData(dataList);

        List<Map<String, Object>> oldList = new ArrayList<>();
        Map<String, Object> old = new LinkedHashMap<>();
        oldList.add(old);
        old.put("role_name", "admin");
        dml.setOld(oldList);

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertEquals("admin2", response.getSource().get("_role_name"));
    }

    /**
     * 主表更新外键值
     */
    @Test
    public void test03() {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds, "delete from role where id=2");
        Common.sqlExe(ds, "insert into role (id,role_name) values (2,'operator')");
        Common.sqlExe(ds, "update user set role_id=2 where id=1");

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
        data.put("role_id", 2L);
        dml.setData(dataList);
        List<Map<String, Object>> oldList = new ArrayList<>();
        Map<String, Object> old = new LinkedHashMap<>();
        oldList.add(old);
        old.put("role_id", 1L);
        dml.setOld(oldList);

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertEquals("operator", response.getSource().get("_role_name"));

        Common.sqlExe(ds, "update user set role_id=1 where id=1");

        Dml dml2 = new Dml();
        dml2.setDestination("example");
        dml2.setTs(new Date().getTime());
        dml2.setType("UPDATE");
        dml2.setDatabase("mytest");
        dml2.setTable("user");
        List<Map<String, Object>> dataList2 = new ArrayList<>();
        Map<String, Object> data2 = new LinkedHashMap<>();
        dataList2.add(data2);
        data2.put("id", 1L);
        data2.put("role_id", 1L);
        dml2.setData(dataList2);
        List<Map<String, Object>> oldList2 = new ArrayList<>();
        Map<String, Object> old2 = new LinkedHashMap<>();
        oldList2.add(old2);
        old2.put("role_id", 2L);
        dml2.setOld(oldList2);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml2);

        GetResponse response2 = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertEquals("admin2", response2.getSource().get("_role_name"));
    }

    /**
     * 非子查询从表删除
     */
    @Test
    public void test04() {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds, "delete from role where id=1");

        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("DELETE");
        dml.setDatabase("mytest");
        dml.setTable("role");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 1L);
        data.put("role_name", "admin");

        dml.setData(dataList);

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertNull(response.getSource().get("_role_name"));
    }
}

package com.alibaba.otter.canal.client.adapter.es6x.test.sync;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
public class RoleSyncJoinOne2Test {

    private ES6xAdapter esAdapter;

    @Before
    public void init() {
        // AdapterConfigs.put("es", "mytest_user_join_one2.yml");
        esAdapter = Common.init();
    }

    /**
     * 带函数非子查询从表插入
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
        Assert.assertEquals("admin_", response.getSource().get("_role_name"));
    }

    /**
     * 带函数非子查询从表更新
     */
    @Test
    public void test02() {
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

        String database = dml.getDatabase();
        String table = dml.getTable();
        Map<String, ESSyncConfig> esSyncConfigs = esAdapter.getDbTableEsSyncConfig().get(database + "-" + table);

        esAdapter.getEsSyncService().sync(esSyncConfigs.values(), dml);

        GetResponse response = esAdapter.getEsConnection()
            .getTransportClient()
            .prepareGet("mytest_user", "_doc", "1")
            .get();
        Assert.assertEquals("admin3_", response.getSource().get("_role_name"));
    }
}

package com.alibaba.otter.canal.client.adapter.es.test.sync;

import java.util.*;

import org.elasticsearch.action.get.GetResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.es.ESAdapter;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

import javax.sql.DataSource;

public class RoleSyncJoinOne2Test {

    private ESAdapter esAdapter;

    @Before
    public void init() {
        AdapterConfigs.put("es", "mytest_user_join_one2.yml");
        esAdapter = Common.init();
    }

    /**
     * 带函数非子查询从表插入
     */
    @Test
    public void test01() {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds,"delete from role where id=1");
        Common.sqlExe(ds,"insert into role (id,role_name) values (1,'admin')");

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

        esAdapter.getEsSyncService().sync(dml);

        GetResponse response = esAdapter.getTransportClient().prepareGet("mytest_user", "_doc", "1").get();
        Assert.assertEquals("admin_", response.getSource().get("_role_name"));
    }

    /**
     * 带函数非子查询从表更新
     */
    @Test
    public void test02() {
        DataSource ds = DatasourceConfig.DATA_SOURCES.get("defaultDS");
        Common.sqlExe(ds,"update role set role_name='admin3' where id=1");

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

        esAdapter.getEsSyncService().sync(dml);

        GetResponse response = esAdapter.getTransportClient().prepareGet("mytest_user", "_doc", "1").get();
        Assert.assertEquals("admin3_", response.getSource().get("_role_name"));
    }
}

package com.alibaba.otter.canal.client.adapter.kudu.test.sync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.kudu.KuduAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;

/**
 * @description
 */
public class TestSyncKudu {

    private KuduAdapter kuduAdapter;

    @Before
    public void init() {
        kuduAdapter = Common.init();
    }

    @Test
    public void testEtl() {
        List<String> param = new ArrayList<>();
        kuduAdapter.etl("kudutest_user.yml", param);
    }

    @Test
    public void testCount() {
        kuduAdapter.count("kudutest_user.yml");
    }

    @Test
    public void testSync() {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setGroupId("g1");
        dml.setTs(new Date().getTime());
        dml.setType("DELETE");
        dml.setDatabase("test1");
        dml.setTable("user");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("id", 1L);
        data.put("name", "liuyadong");
        data.put("role_id", 1L);
        data.put("c_time", new Date());
        dml.setData(dataList);

        kuduAdapter.sync(Collections.singletonList(dml));
    }
}

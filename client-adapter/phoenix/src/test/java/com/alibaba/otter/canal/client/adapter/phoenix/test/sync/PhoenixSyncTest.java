package com.alibaba.otter.canal.client.adapter.phoenix.test.sync;

import com.alibaba.otter.canal.client.adapter.phoenix.PhoenixAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * @author: lihua
 * @date: 2021/1/5 23:16
 * @Description:
 */
public class PhoenixSyncTest {

    private PhoenixAdapter phoenixAdapter;

    @Before
    public void init() {
        phoenixAdapter = Common.init();
    }

    @Test
    public void testEtl() {
        List<String> param = new ArrayList<>();
        phoenixAdapter.etl("phoenixtest_user.yml", param);
    }
    @Test
    public void testCount() {
        phoenixAdapter.count("phoenixtest_user.yml");
    }
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
        data.put("id", 1);
        data.put("name", "sixPulseExcalibur");
        data.put("password", "123456");
        dml.setData(dataList);

        phoenixAdapter.sync(Collections.singletonList(dml));
    }

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
        data.put("id", 1);
        data.put("name", "sixPulseExcalibur2");
        dml.setData(dataList);
        List<Map<String, Object>> oldList = new ArrayList<>();
        Map<String, Object> old = new LinkedHashMap<>();
        oldList.add(old);
        old.put("name", "sixPulseExcalibur");
        dml.setOld(oldList);
        phoenixAdapter.sync(Collections.singletonList(dml));
    }
}

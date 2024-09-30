package com.alibaba.otter.canal.client.adapter.clickhouse;

import java.util.*;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.clickhouse.sync.Common;
import com.alibaba.otter.canal.client.adapter.support.Dml;

@Ignore
public class ClickHouseBatchSyncServiceTest {

    private ClickHouseAdapter clickHouseAdapter;

    @Before
    public void init() {
        clickHouseAdapter = Common.init();
    }

    @Test
    public void insert() throws InterruptedException {
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
        data.put("test1", "sdfasdfawe中国asfwef");
        dml.setData(dataList);

        clickHouseAdapter.sync(Collections.singletonList(dml));
        Thread.sleep(10000L);
    }

    @Test
    public void update() {
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

        clickHouseAdapter.sync(Collections.singletonList(dml));
    }

    @Test
    public void delete() {
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
        data.put("name", "Eric2");
        dml.setData(dataList);

        clickHouseAdapter.sync(Collections.singletonList(dml));
    }

    @Test
    public void truncate() throws InterruptedException {
        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("TRUNCATE");
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

        clickHouseAdapter.sync(Collections.singletonList(dml));
        Thread.sleep(1000L);

    }
}

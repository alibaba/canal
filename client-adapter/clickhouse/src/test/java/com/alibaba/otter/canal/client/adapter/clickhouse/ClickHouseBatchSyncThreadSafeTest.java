package com.alibaba.otter.canal.client.adapter.clickhouse;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.clickhouse.sync.Common;
import com.alibaba.otter.canal.client.adapter.support.Dml;

import ch.qos.logback.classic.Level;

/**
 * @author: Xander
 * @date: Created in 2023/11/13 22:27
 * @email: zhrunxin33@gmail.com
 * @description: Testing thread safe
 */

@Ignore
public class ClickHouseBatchSyncThreadSafeTest {

    private ClickHouseAdapter clickHouseAdapter;

    private ExecutorService   executorService;

    private String[]          operations = new String[] { "INSERT", "UPDATE" };

    private String[]          tables     = new String[] { "user", "customer" };

    @Before
    public void init() {
        clickHouseAdapter = Common.init();
        Common.setLogLevel(Level.INFO);
        executorService = Executors.newFixedThreadPool(5);
    }

    @Test
    public void test01() throws InterruptedException, ExecutionException {
        ArrayList<Future> list = new ArrayList();
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            list.add(executorService.submit(() -> {
                for (int j = 0; j < 300; j++) {
                    Random random = new Random();
                    int cou = count.incrementAndGet();
                    // test insert
                    String dmlType = operations[random.nextInt(1)];
                    Dml dml = new Dml();
                    dml.setDestination("example");
                    dml.setTs(new Date().getTime());
                    dml.setType(dmlType);
                    dml.setDatabase("mytest");
                    dml.setTable(tables[(int) Math.round(Math.random())]);
                    List<Map<String, Object>> dataList = new ArrayList<>();
                    Map<String, Object> data = new LinkedHashMap<>();
                    dataList.add(data);
                    data.put("id", cou);
                    data.put("name", "Eric" + cou);
                    data.put("role_id", cou);
                    data.put("c_time", new Date());
                    data.put("test1", "sdfasdfawe中国asfwef");
                    dml.setData(dataList);
                    clickHouseAdapter.sync(Collections.singletonList(dml));
                }
            }));

        }
        for (Future future : list) {
            future.get();
        }
        Thread.sleep(10000L); // waiting multiple threads execute successfully.
    }

}

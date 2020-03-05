package com.alibaba.otter.canal.client.adapter.kudu.test.sync;

import com.alibaba.otter.canal.client.adapter.kudu.KuduAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　┻　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃  神兽保佑
 * 　　　　┃　　　┃  代码无bug
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * Created by Liuyadong on 2019-11-13
 *
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
    public void testCount(){
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

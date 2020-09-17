package com.alibaba.otter.canal.client.adapter.kudu.test.sync;

import com.alibaba.otter.canal.client.adapter.kudu.KuduAdapter;
import com.alibaba.otter.canal.client.adapter.kudu.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

import java.util.HashMap;
import java.util.Map;

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
public class Common {
    public static KuduAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("kudu");
        outerAdapterConfig.setKey("kudu");
        Map<String, String> properties = new HashMap<>();
        properties.put("kudu.master.address", "10.6.36.102,10.6.36.187,10.6.36.229");
        outerAdapterConfig.setProperties(properties);

        KuduAdapter adapter = new KuduAdapter();
        adapter.init(outerAdapterConfig, null);
        return adapter;
    }
}

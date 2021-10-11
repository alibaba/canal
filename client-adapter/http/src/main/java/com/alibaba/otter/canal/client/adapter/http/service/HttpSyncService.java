/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.http.support.HttpTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public class HttpSyncService {

    private static Logger logger = LoggerFactory.getLogger(HttpSyncService.class);

    private HttpTemplate httpTemplate;

    public HttpSyncService(HttpTemplate httpTemplate) {
        this.httpTemplate = httpTemplate;
    }

    public void sync(MappingConfig config, List<Dml> dmls) {
        if (config != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("HttpSyncService.sync: config: {}, dml: {}",
                        JSON.toJSONString(config, SerializerFeature.WriteMapNullValue),
                        JSON.toJSONString(dmls, SerializerFeature.WriteMapNullValue));
            }

            List<Map<String, Object>> list = new ArrayList<>();

            for (Dml dml : dmls) {
                String type = dml.getType();
                String database = dml.getDatabase();
                String table = dml.getTable();
                List<Map<String, Object>> dataList = dml.getData();

                // 这边有必要过滤掉数据操作以外的DML
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    type = "insert";
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    type = "update";
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    type = "delete";
                }

                if (type != null && dataList.size() > 0) {
                    Map<String, Object> data = dataList.get(0);
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("database", database);
                    item.put("table", table);
                    item.put("action", type);
                    item.put("data", data);
                    list.add(item);
                }
            }

            if (list.size() > 0) {
                this.httpTemplate.runAsync("sync", list);
            }
        }
    }
}

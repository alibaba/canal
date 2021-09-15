/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.service;

import java.util.ArrayList;
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

    public void sync(MappingConfig config, Dml dml) {
        if (config != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("HttpSyncService.sync: config: {}, dml: ",
                        JSON.toJSONString(config, SerializerFeature.WriteMapNullValue),
                        JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }

            String type = dml.getType();
            String database = dml.getDatabase();
            String tableName = dml.getTable();
            String tableFullName = database + "." + tableName;

            if (type != null && type.equalsIgnoreCase("INSERT")) {
                List<Map<String, Object>> dataList = new ArrayList<>();
                httpTemplate.insert(tableFullName, dataList);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                List<Map<String, Object>> dataList = new ArrayList<>();
                httpTemplate.update(tableFullName, dataList);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                List<Map<String, Object>> dataList = new ArrayList<>();
                httpTemplate.insert(tableFullName, dataList);
            }
        }
    }
}

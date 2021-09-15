/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.support;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpTemplate {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String serviceUrl;

    public HttpTemplate(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public void insert(String tableName, List<Map<String, Object>> dataList) {
        if (logger.isDebugEnabled()) {
            logger.debug("INSERT: tableName: {}, dataList: {}, serviceUrl: {}", tableName, JSON.toJSONString(dataList),
                    this.serviceUrl);
        }
    }

    public void update(String tableName, List<Map<String, Object>> dataList) {
        if (logger.isDebugEnabled()) {
            logger.debug("UPDATE: tableName: {}, dataList: {}, serviceUrl: {}", tableName, JSON.toJSONString(dataList),
                    this.serviceUrl);
        }
    }

    public void delete(String tableName, List<Map<String, Object>> dataList) {
        if (logger.isDebugEnabled()) {
            logger.debug("DELETE: tableName: {}, dataList: {}, serviceUrl: {}", tableName, JSON.toJSONString(dataList),
                    this.serviceUrl);
        }
    }
}

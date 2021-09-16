/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.support;

import java.util.Map;
import java.util.LinkedHashMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.kevinsawicki.http.HttpRequest;

public class HttpTemplate {
    private String serviceUrl;
    private String sign;

    public HttpTemplate(String serviceUrl, String sign) {
        this.serviceUrl = serviceUrl;
        this.sign = sign;
    }

    public void insert(String database, String table, Map<String, Object> data) {
        new Task(this.serviceUrl, this.sign, database, table, data, "insert").start();
    }

    public void update(String database, String table, Map<String, Object> data) {
        new Task(this.serviceUrl, this.sign, database, table, data, "update").start();

    }

    public void delete(String database, String table, Map<String, Object> data) {
        new Task(this.serviceUrl, this.sign, database, table, data, "delete").start();
    }

    public static class Task extends Thread {
        private Logger logger = LoggerFactory.getLogger(this.getClass());

        private String serviceUrl;
        private String sign;
        private String database;
        private String table;
        private String action;
        private Map<String, Object> data;

        public Task(String serviceUrl, String sign, String database, String table, Map<String, Object> data,
                String action) {
            this.serviceUrl = serviceUrl;
            this.sign = sign;
            this.database = database;
            this.table = table;
            this.data = data;
            this.action = action;
        }

        public void run() {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("database", this.database);
            body.put("table", this.table);
            body.put("action", this.action);
            body.put("data", this.data);
            body.put("sign", this.sign);

            if (logger.isDebugEnabled()) {
                logger.debug("{}", JSON.toJSONString(body, SerializerFeature.WriteMapNullValue));
            }

            HttpRequest.post(this.serviceUrl).contentType("application/json;charset=UTF-8")
                    .send(JSON.toJSONString(body, SerializerFeature.WriteMapNullValue)).code();
        }
    }
}

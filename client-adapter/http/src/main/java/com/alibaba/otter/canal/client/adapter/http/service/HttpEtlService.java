/**
 * Created by Wu Jian Ping on - 2021/09/15.
 */

package com.alibaba.otter.canal.client.adapter.http.service;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.http.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.http.support.HttpTemplate;
import com.alibaba.otter.canal.client.adapter.support.AbstractEtlService;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.Util;

public class HttpEtlService extends AbstractEtlService {

    private HttpTemplate httpTemplate;
    private MappingConfig config;

    public HttpEtlService(HttpTemplate httpTemplate, MappingConfig config) {
        super("http", config);
        this.httpTemplate = httpTemplate;
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        EtlResult etlResult = new EtlResult();
        etlResult.setSucceeded(true);
        return etlResult;
    }

    @Override
    protected boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
            AdapterConfig.AdapterMapping mapping, AtomicLong impCount, List<String> errMsg) {

        MappingConfig.HttpMapping httpMapping = (MappingConfig.HttpMapping) mapping;

        Map<String, String> columnsMap = new LinkedHashMap<>();// 需要同步的字段

        try {
            Util.sqlRS(ds, "SELECT * FROM USER" + " LIMIT 1", rs -> {
                try {
                    return true;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            });
            // 写入数据
            logger.info("etl select data sql is :{}", sql);
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }

    }
}

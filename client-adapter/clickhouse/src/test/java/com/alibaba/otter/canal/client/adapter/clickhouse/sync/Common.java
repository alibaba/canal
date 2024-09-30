package com.alibaba.otter.canal.client.adapter.clickhouse.sync;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.clickhouse.ClickHouseAdapter;
import com.alibaba.otter.canal.client.adapter.clickhouse.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * @author: Xander
 * @date: Created in 2023/11/13 0:16
 * @email: zhrunxin33@gmail.com @description：
 */
public class Common {

    public static ClickHouseAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("clickhouse");
        outerAdapterConfig.setKey("clickhouse1");
        Map<String, String> properties = new HashMap<>();
        properties.put("jdbc.driveClassName", "ru.yandex.clickhouse.ClickHouseDriver");
        properties.put("jdbc.url", "jdbc:clickhouse://127.0.0.1:8123/default");
        properties.put("jdbc.username", "default");
        properties.put("jdbc.password", "123456");
        outerAdapterConfig.setProperties(properties);

        ClickHouseAdapter adapter = new ClickHouseAdapter();
        adapter.init(outerAdapterConfig, null);
        return adapter;
    }

    /**
     * set global log level
     *
     * @param logLevel
     */
    public static void setLogLevel(Level logLevel) {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory
            .getLogger(Logger.ROOT_LOGGER_NAME);
        logger.setLevel(logLevel);
    }
}

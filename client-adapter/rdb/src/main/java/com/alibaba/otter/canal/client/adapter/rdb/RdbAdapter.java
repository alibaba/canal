package com.alibaba.otter.canal.client.adapter.rdb;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfigLoader;
import com.alibaba.otter.canal.client.adapter.rdb.service.RdbSyncService;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;

@SPI("rdb")
public class RdbAdapter implements OuterAdapter {

    private static Logger                              logger             = LoggerFactory.getLogger(RdbAdapter.class);

    private static volatile Map<String, MappingConfig> rdbMapping         = null;                                     // 文件名对应配置
    private static volatile Map<String, MappingConfig> mappingConfigCache = null;                                     // 库名-表名对应配置

    private DruidDataSource                            dataSource;

    private RdbSyncService                             rdbSyncService;

    @Override
    public void init(OuterAdapterConfig configuration) {
        if (mappingConfigCache == null) {
            synchronized (MappingConfig.class) {
                if (mappingConfigCache == null) {
                    rdbMapping = MappingConfigLoader.load();
                    mappingConfigCache = new HashMap<>();
                    for (MappingConfig mappingConfig : rdbMapping.values()) {
                        mappingConfigCache.put(StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                                               + mappingConfig.getDbMapping().getDatabase() + "."
                                               + mappingConfig.getDbMapping().getTable(),
                            mappingConfig);
                    }
                }
            }
        }

        Map<String, String> properties = configuration.getProperties();
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(2);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        rdbSyncService = new RdbSyncService(dataSource);
    }

    @Override
    public void sync(Dml dml) {
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String database = dml.getDatabase();
        String table = dml.getTable();
        MappingConfig config = mappingConfigCache.get(destination + "." + database + "." + table);
        rdbSyncService.sync(config, dml);
    }

    @Override
    public void destroy() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}

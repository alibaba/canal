package com.alibaba.otter.canal.adapter.launcher.config;

import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

/**
 * canal 的相关配置类
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@Component
@ConfigurationProperties(prefix = "canal.conf")
public class AdapterCanalConfig extends CanalClientConfig {

    public final Set<String>              DESTINATIONS = new LinkedHashSet<>();

    private Map<String, DatasourceConfig> srcDataSources;

    @Override
    public void setCanalAdapters(List<CanalAdapter> canalAdapters) {
        super.setCanalAdapters(canalAdapters);

        if (canalAdapters != null) {
            synchronized (DESTINATIONS) {
                DESTINATIONS.clear();
                for (CanalAdapter canalAdapter : canalAdapters) {
                    if (canalAdapter.getInstance() != null) {
                        DESTINATIONS.add(canalAdapter.getInstance());
                    }
                }
            }
        }
    }

    public Map<String, DatasourceConfig> getSrcDataSources() {
        return srcDataSources;
    }

    @SuppressWarnings("resource")
    public void setSrcDataSources(Map<String, DatasourceConfig> srcDataSources) {
        this.srcDataSources = srcDataSources;

        if (srcDataSources != null) {
            for (Map.Entry<String, DatasourceConfig> entry : srcDataSources.entrySet()) {
                DatasourceConfig datasourceConfig = entry.getValue();
                // 加载数据源连接池
                DruidDataSource ds = new DruidDataSource();
                ds.setDriverClassName(datasourceConfig.getDriver());
                ds.setUrl(datasourceConfig.getUrl());
                ds.setUsername(datasourceConfig.getUsername());
                ds.setPassword(datasourceConfig.getPassword());
                ds.setInitialSize(1);
                ds.setMinIdle(1);
                ds.setMaxActive(datasourceConfig.getMaxActive());
                ds.setMaxWait(60000);
                ds.setTimeBetweenEvictionRunsMillis(60000);
                ds.setMinEvictableIdleTimeMillis(300000);
                ds.setValidationQuery("select 1");
                try {
                    ds.init();
                } catch (SQLException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
                DatasourceConfig.DATA_SOURCES.put(entry.getKey(), ds);
            }
        }
    }
}

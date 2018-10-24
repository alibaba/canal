package com.alibaba.otter.canal.adapter.launcher.config;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

@Component
@ConfigurationProperties(prefix = "adapter.conf")
public class AdapterConfig {

    private Map<String, DatasourceConfig> datasourceConfigs;

    private List<String>                  adapterConfigs;

    public List<String> getAdapterConfigs() {
        return adapterConfigs;
    }

    public Map<String, DatasourceConfig> getDatasourceConfigs() {
        return datasourceConfigs;
    }

    public void setDatasourceConfigs(Map<String, DatasourceConfig> datasourceConfigs) {
        this.datasourceConfigs = datasourceConfigs;

        if (datasourceConfigs != null) {
            for (Map.Entry<String, DatasourceConfig> entry : datasourceConfigs.entrySet()) {
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
                ds.setPoolPreparedStatements(false);
                ds.setMaxPoolPreparedStatementPerConnectionSize(20);
                ds.setValidationQuery("select 1");
                try {
                    ds.init();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                DatasourceConfig.DATA_SOURCES.put(entry.getKey(), ds);
            }
        }
    }

    public void setAdapterConfigs(List<String> adapterConfigs) {
        this.adapterConfigs = adapterConfigs;

        if (adapterConfigs != null) {
            AdapterConfigs.clear();
            for (String adapterConfig : adapterConfigs) {
                int idx = adapterConfig.indexOf("/");
                if (idx > -1) {
                    String type = adapterConfig.substring(0, idx);
                    String ymlFile = adapterConfig.substring(idx + 1);
                    AdapterConfigs.put(type, ymlFile);
                }
            }
        }
    }
}

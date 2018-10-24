package com.alibaba.otter.canal.adapter.launcher.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfigs;

@Component
@ConfigurationProperties(prefix = "adapter.conf.path")
public class AdapterConfigPath {

    private List<String> adapterConfigs;

    public List<String> getAdapterConfigs() {
        return adapterConfigs;
    }

    public void setAdapterConfigs(List<String> adapterConfigs) {
        this.adapterConfigs = adapterConfigs;

        if (adapterConfigs != null) {
            AdapterConfigs.configs.clear();
            for (String adapterConfig : adapterConfigs) {
                int idx = adapterConfig.indexOf("/");
                if (idx > -1) {
                    String type = adapterConfig.substring(0, idx);
                    String ymlFile = adapterConfig.substring(idx + 1);
                    AdapterConfigs.configs.put(type, ymlFile);
                }
            }
        }
    }
}

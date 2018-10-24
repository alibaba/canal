package com.alibaba.otter.canal.adapter.launcher.config;

import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "canal.conf")
@Component
public class AdapterCanalConfig extends CanalClientConfig {
}

package com.alibaba.otter.canal.adapter.launcher.config;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;

/**
 * canal 的相关配置类
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@Component
@ConfigurationProperties(prefix = "canal.conf")
public class AdapterCanalConfig extends CanalClientConfig {

    public final Set<String> DESTINATIONS = new LinkedHashSet<>();

    @Override
    public void setCanalInstances(List<CanalInstance> canalInstances) {
        super.setCanalInstances(canalInstances);

        if (canalInstances != null) {
            synchronized (DESTINATIONS) {
                DESTINATIONS.clear();
                for (CanalInstance canalInstance : canalInstances) {
                    DESTINATIONS.add(canalInstance.getInstance());
                }
            }
        }
    }

    @Override
    public void setMqTopics(List<MQTopic> mqTopics) {
        super.setMqTopics(mqTopics);

        if (mqTopics != null) {
            synchronized (DESTINATIONS) {
                DESTINATIONS.clear();
                for (MQTopic mqTopic : mqTopics) {
                    DESTINATIONS.add(mqTopic.getTopic());
                }
            }
        }
    }
}

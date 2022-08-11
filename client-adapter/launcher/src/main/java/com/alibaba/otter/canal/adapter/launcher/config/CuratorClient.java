package com.alibaba.otter.canal.adapter.launcher.config;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.stereotype.Component;

/**
 * curator 配置类
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@Component
public class CuratorClient {

    @Resource
    private AdapterCanalConfig adapterCanalConfig;

    private CuratorFramework   curator = null;

    @PostConstruct
    public void init() {
        if (StringUtils.isNotEmpty(adapterCanalConfig.getZookeeperHosts())) {
            curator = CuratorFrameworkFactory.builder()
                .connectString(adapterCanalConfig.getZookeeperHosts())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
                .namespace("canal-adapter")
                .build();
            curator.start();
        }
    }

    public CuratorFramework getCurator() {
        return curator;
    }
}

package com.alibaba.otter.canal.client.adapter.support;

import java.util.Map;

/**
 * 外部适配器配置信息类
 *
 * @author rewerma 2018-8-18 下午10:15:12
 * @version 1.0.0
 */
public class OuterAdapterConfig {

    private String              name;       // 适配器名称, 如: logger, hbase, es

    private String              key;        // 适配器唯一键

    private String              hosts;      // 适配器内部的地址, 比如对应es该参数可以填写es的server地址

    private String              zkHosts;    // 适配器内部的ZK地址, 比如对应HBase该参数可以填写HBase对应的ZK地址

    private Map<String, String> properties; // 其余参数, 可填写适配器中的所需的配置信息

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    public void setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
    }
}

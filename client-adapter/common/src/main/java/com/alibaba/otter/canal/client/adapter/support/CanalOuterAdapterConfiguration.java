package com.alibaba.otter.canal.client.adapter.support;

import java.util.Properties;

/**
 * 外部适配器配置信息类
 *
 * @author machengyuan 2018-8-18 下午10:15:12
 * @version 1.0.0
 */
public class CanalOuterAdapterConfiguration {

    private String     name;      // 适配器名称, 如: logger, hbase, es

    private String     hosts;     // 适配器内部的地址, 比如对应es该参数可以填写es的server地址

    private String     zkHosts;   // 适配器内部的ZK地址, 比如对应HBase该参数可以填写HBase对应的ZK地址

    private Properties properties; // 其余参数, 可填写适配器中的所需的配置信息

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    public void setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
    }
}

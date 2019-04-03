package com.alibaba.otter.canal.client.adapter.support;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置信息类
 *
 * @author rewerma 2018-8-18 下午10:40:12
 * @version 1.0.0
 */
public class CanalClientConfig {

    // 单机模式下canal server的ip:port
    private String             canalServerHost;
    // 集群模式下的zk地址,如果配置了单机地址则以单机为准!!
    private String             zookeeperHosts;
    // kafka or rocket mq 地址
    private String             mqServers;
    // 是否已flatMessage模式传输,只适用于mq模式
    private Boolean            flatMessage   = true;
    // 批大小
    private Integer            batchSize;
    // 同步分批提交大小
    private Integer            syncBatchSize = 1000;
    // 重试次数
    private Integer            retries;
    // 消费超时时间
    private Long               timeout;
    // 模式 tcp kafka rocketMQ
    private String             mode          = "tcp";
    // aliyun ak/sk
    private String             accessKey;
    private String             secretKey;

    // canal adapters 配置
    private List<CanalAdapter> canalAdapters;

    public String getCanalServerHost() {
        return canalServerHost;
    }

    public void setCanalServerHost(String canalServerHost) {
        this.canalServerHost = canalServerHost;
    }

    public String getZookeeperHosts() {
        return zookeeperHosts;
    }

    public void setZookeeperHosts(String zookeeperHosts) {
        this.zookeeperHosts = zookeeperHosts;
    }

    public String getMqServers() {
        return mqServers;
    }

    public void setMqServers(String mqServers) {
        this.mqServers = mqServers;
    }

    public Boolean getFlatMessage() {
        return flatMessage;
    }

    public void setFlatMessage(Boolean flatMessage) {
        this.flatMessage = flatMessage;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getRetries() {
        return retries;
    }

    public Integer getSyncBatchSize() {
        return syncBatchSize;
    }

    public void setSyncBatchSize(Integer syncBatchSize) {
        this.syncBatchSize = syncBatchSize;
    }

    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public List<CanalAdapter> getCanalAdapters() {
        return canalAdapters;
    }

    public void setCanalAdapters(List<CanalAdapter> canalAdapters) {
        this.canalAdapters = canalAdapters;
    }

    public static class CanalAdapter {

        private String      instance; // 实例名

        private List<Group> groups;  // 适配器分组列表

        public String getInstance() {
            return instance;
        }

        public void setInstance(String instance) {
            if (instance != null) {
                this.instance = instance.trim();
            }
        }

        public List<Group> getGroups() {
            return groups;
        }

        public void setGroups(List<Group> groups) {
            this.groups = groups;
        }
    }

    public static class Group {

        // group id
        private String                          groupId          = "default";
        private List<OuterAdapterConfig>        outerAdapters;                           // 适配器列表
        private Map<String, OuterAdapterConfig> outerAdaptersMap = new LinkedHashMap<>();

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public List<OuterAdapterConfig> getOuterAdapters() {
            return outerAdapters;
        }

        public void setOuterAdapters(List<OuterAdapterConfig> outerAdapters) {
            this.outerAdapters = outerAdapters;
        }

        public Map<String, OuterAdapterConfig> getOuterAdaptersMap() {
            return outerAdaptersMap;
        }

        public void setOuterAdaptersMap(Map<String, OuterAdapterConfig> outerAdaptersMap) {
            this.outerAdaptersMap = outerAdaptersMap;
        }
    }
}

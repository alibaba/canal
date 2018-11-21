package com.alibaba.otter.canal.client.adapter.support;

import java.util.ArrayList;
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

    private String              canalServerHost;      // 单机模式下canal server的 ip:port

    private String              zookeeperHosts;       // 集群模式下的zk地址, 如果配置了单机地址则以单机为准!!

    private String              mqServers;            // kafka or rocket mq 地址

    private Boolean             flatMessage   = true; // 是否已flatMessage模式传输, 只适用于mq模式

    private Integer             batchSize;            // 批大小

    private Integer             syncBatchSize = 1000; // 同步分批提交大小

    private Integer             retries;              // 重试次数

    private Long                timeout;              // 消费超时时间

    private List<MQTopic>       mqTopics;             // mq topic 列表

    private List<CanalInstance> canalInstances;       // tcp 模式下 canal 实例列表, 与mq模式不能共存!!

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

    public List<MQTopic> getMqTopics() {
        return mqTopics;
    }

    public void setMqTopics(List<MQTopic> mqTopics) {
        this.mqTopics = mqTopics;
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

    public List<CanalInstance> getCanalInstances() {
        return canalInstances;
    }

    public void setCanalInstances(List<CanalInstance> canalInstances) {
        this.canalInstances = canalInstances;
    }

    public static class CanalInstance {

        private String      instance; // 实例名

        private List<Group> groups;   // 适配器分组列表

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

        private List<OuterAdapterConfig>        outAdapters;                           // 适配器列表

        private Map<String, OuterAdapterConfig> outAdaptersMap = new LinkedHashMap<>();

        public List<OuterAdapterConfig> getOutAdapters() {
            return outAdapters;
        }

        public void setOutAdapters(List<OuterAdapterConfig> outAdapters) {
            this.outAdapters = outAdapters;
            if (outAdapters != null) {
                outAdapters.forEach(outAdapter -> outAdaptersMap.put(outAdapter.getKey(), outAdapter));
            }
        }

        public Map<String, OuterAdapterConfig> getOutAdaptersMap() {
            return outAdaptersMap;
        }

        public void setOutAdaptersMap(Map<String, OuterAdapterConfig> outAdaptersMap) {
            this.outAdaptersMap = outAdaptersMap;
        }
    }

    public static class MQTopic {

        private String        mqMode;                     // mq模式 kafka or rocketMQ

        private String        topic;                      // topic名

        private List<MQGroup> groups = new ArrayList<>(); // 分组列表

        public String getMqMode() {
            return mqMode;
        }

        public void setMqMode(String mqMode) {
            this.mqMode = mqMode;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public List<MQGroup> getGroups() {
            return groups;
        }

        public void setGroups(List<MQGroup> groups) {
            this.groups = groups;
        }
    }

    public static class MQGroup {

        private String                   groupId;     // group id

        private List<OuterAdapterConfig> outAdapters; // 适配器配置列表

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public List<OuterAdapterConfig> getOutAdapters() {
            return outAdapters;
        }

        public void setOutAdapters(List<OuterAdapterConfig> outAdapters) {
            this.outAdapters = outAdapters;
        }

    }

}

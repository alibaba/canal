package com.alibaba.otter.canal.client.support;

import com.alibaba.otter.canal.client.adapter.CanalOuterAdapterConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 配置信息类
 *
 * @author machengyuan 2018-8-18 下午10:40:12
 * @version 1.0.0
 */
public class CanalClientConfig {
    private String canalServerHost;

    private String zookeeperHosts;

    private Properties properties;

    private String bootstrapServers;

    private List<KafkaTopic> kafkaTopics = new ArrayList<>();

    private List<CanalInstance> canalInstances = new ArrayList<>();

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

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public List<KafkaTopic> getKafkaTopics() {
        return kafkaTopics;
    }

    public void setKafkaTopics(List<KafkaTopic> kafkaTopics) {
        this.kafkaTopics = kafkaTopics;
    }

    public List<CanalInstance> getCanalInstances() {
        return canalInstances;
    }

    public void setCanalInstances(List<CanalInstance> canalInstances) {
        this.canalInstances = canalInstances;
    }

    public static class CanalInstance {
        private String instance;

        private List<AdapterGroup> adapterGroups;

        public String getInstance() {
            return instance;
        }

        public void setInstance(String instance) {
            if (instance != null) {
                this.instance = instance.trim();
            }
        }

        public List<AdapterGroup> getAdapterGroups() {
            return adapterGroups;
        }

        public void setAdapterGroups(List<AdapterGroup> adapterGroups) {
            this.adapterGroups = adapterGroups;
        }
    }

    public static class AdapterGroup {
        private List<CanalOuterAdapterConfiguration> outAdapters;

        public List<CanalOuterAdapterConfiguration> getOutAdapters() {
            return outAdapters;
        }

        public void setOutAdapters(List<CanalOuterAdapterConfiguration> outAdapters) {
            this.outAdapters = outAdapters;
        }
    }

    public static class KafkaTopic {
        private String topic;

        private List<Group> groups = new ArrayList<>();

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public List<Group> getGroups() {
            return groups;
        }

        public void setGroups(List<Group> groups) {
            this.groups = groups;
        }
    }

    public static class Group {
        private String groupId;

//        private List<Adaptor> adapters = new ArrayList<>();

        private List<CanalOuterAdapterConfiguration> outAdapters;

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public List<CanalOuterAdapterConfiguration> getOutAdapters() {
            return outAdapters;
        }

        public void setOutAdapters(List<CanalOuterAdapterConfiguration> outAdapters) {
            this.outAdapters = outAdapters;
        }

        //        public List<Adaptor> getAdapters() {
//            return adapters;
//        }
//
//        public void setAdapters(List<Adaptor> adapters) {
//            this.adapters = adapters;
//        }
    }

//    public static class Adaptor {
//        private List<CanalOuterAdapterConfiguration> outAdapters;
//
//        public List<CanalOuterAdapterConfiguration> getOutAdapters() {
//            return outAdapters;
//        }
//
//        public void setOutAdapters(List<CanalOuterAdapterConfiguration> outAdapters) {
//            this.outAdapters = outAdapters;
//        }
//    }
}

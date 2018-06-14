package com.alibaba.otter.canal.kafka.producer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * kafka 配置项
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class KafkaProperties {

    private String                 servers           = "localhost:6667";
    private int                    retries           = 0;
    private int                    batchSize         = 16384;
    private int                    lingerMs          = 1;
    private long                   bufferMemory      = 33554432L;

    private int                    canalBatchSize    = 5;

    private List<CanalDestination> canalDestinations = new ArrayList<CanalDestination>();

    public static class CanalDestination {

        private String     canalDestination;
        private String     topic;
        private Integer    partition;
        private Set<Topic> topics = new HashSet<Topic>();

        public String getCanalDestination() {
            return canalDestination;
        }

        public void setCanalDestination(String canalDestination) {
            this.canalDestination = canalDestination;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public Set<Topic> getTopics() {
            return topics;
        }

        public void setTopics(Set<Topic> topics) {
            this.topics = topics;
        }
    }

    public static class Topic {

        private String  topic;
        private Integer partition;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Topic topic1 = (Topic) o;

            if (topic != null ? !topic.equals(topic1.topic) : topic1.topic != null) return false;
            return partition != null ? partition.equals(topic1.partition) : topic1.partition == null;
        }

        @Override
        public int hashCode() {
            int result = topic != null ? topic.hashCode() : 0;
            result = 31 * result + (partition != null ? partition.hashCode() : 0);
            return result;
        }
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }

    public long getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(long bufferMemory) {
        this.bufferMemory = bufferMemory;
    }

    public int getCanalBatchSize() {
        return canalBatchSize;
    }

    public void setCanalBatchSize(int canalBatchSize) {
        this.canalBatchSize = canalBatchSize;
    }

    public List<CanalDestination> getCanalDestinations() {
        return canalDestinations;
    }

    public void setCanalDestinations(List<CanalDestination> canalDestinations) {
        this.canalDestinations = canalDestinations;
    }
}

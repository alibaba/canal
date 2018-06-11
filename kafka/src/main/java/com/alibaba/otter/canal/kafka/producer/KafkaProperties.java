package com.alibaba.otter.canal.kafka.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * kafka 配置项
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class KafkaProperties {
    private String servers = "localhost:6667";
    private int retries = 0;
    private int batchSize = 16384;
    private int lingerMs = 1;
    private long bufferMemory = 33554432L;

    private List<Topic> topics = new ArrayList<Topic>();
    private Map<String, Topic> topicMap = new HashMap<String, Topic>();

    public static class Topic {
        private String topic;
        private Integer partition;
        private String destination;

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

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
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

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;

        if (topics != null) {
            for (Topic topic : topics) {
                this.topicMap.put(topic.destination, topic);
            }
        }
    }

    public Topic getTopicByDestination(String destination) {
        return this.topicMap.get(destination);
    }
}

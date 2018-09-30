package com.alibaba.otter.canal.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * kafka 配置项
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class MQProperties {

    private String                 servers                = "localhost:6667";
    private int                    retries                = 0;
    private int                    batchSize              = 16384;
    private int                    lingerMs               = 1;
    private long                   bufferMemory           = 33554432L;
    private boolean                filterTransactionEntry = true;
    private String                 producerGroup          = "Canal-Producer";
    private int                    canalBatchSize         = 50;
    private Long                   canalGetTimeout;
    private boolean                flatMessage            = true;

    private List<CanalDestination> canalDestinations      = new ArrayList<CanalDestination>();

    public static class CanalDestination {

        private String              canalDestination;
        private String              topic;
        private Integer             partition;
        private Integer             partitionsNum;
        private Map<String, String> partitionHash;

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

        public Integer getPartitionsNum() {
            return partitionsNum;
        }

        public void setPartitionsNum(Integer partitionsNum) {
            this.partitionsNum = partitionsNum;
        }

        public Map<String, String> getPartitionHash() {
            return partitionHash;
        }

        public void setPartitionHash(Map<String, String> partitionHash) {
            this.partitionHash = partitionHash;
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

    public Long getCanalGetTimeout() {
        return canalGetTimeout;
    }

    public void setCanalGetTimeout(Long canalGetTimeout) {
        this.canalGetTimeout = canalGetTimeout;
    }

    public boolean getFlatMessage() {
        return flatMessage;
    }

    public void setFlatMessage(boolean flatMessage) {
        this.flatMessage = flatMessage;
    }

    public List<CanalDestination> getCanalDestinations() {
        return canalDestinations;
    }

    public void setCanalDestinations(List<CanalDestination> canalDestinations) {
        this.canalDestinations = canalDestinations;
    }

    public boolean isFilterTransactionEntry() {
        return filterTransactionEntry;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}

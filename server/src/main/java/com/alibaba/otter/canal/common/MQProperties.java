package com.alibaba.otter.canal.common;

import java.util.Map;

/**
 * kafka 配置项
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class MQProperties {

    private String  servers                = "127.0.0.1:6667";
    private int     retries                = 0;
    private int     batchSize              = 16384;
    private int     lingerMs               = 1;
    private int     maxRequestSize         = 1048576;
    private long    bufferMemory           = 33554432L;
    private boolean filterTransactionEntry = true;
    private String  producerGroup          = "Canal-Producer";
    private int     canalBatchSize         = 50;
    private Long    canalGetTimeout        = 100L;
    private boolean flatMessage            = true;
    private String  compressionType        = "none";
    private String  acks                   = "all";
    private String  aliyunAccessKey        = "";
    private String  aliyunSecretKey        = "";

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

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public String getAliyunAccessKey() {
        return aliyunAccessKey;
    }

    public void setAliyunAccessKey(String aliyunAccessKey) {
        this.aliyunAccessKey = aliyunAccessKey;
    }

    public String getAliyunSecretKey() {
        return aliyunSecretKey;
    }

    public void setAliyunSecretKey(String aliyunSecretKey) {
        this.aliyunSecretKey = aliyunSecretKey;
    }
    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public void setMaxRequestSize(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
    }
}

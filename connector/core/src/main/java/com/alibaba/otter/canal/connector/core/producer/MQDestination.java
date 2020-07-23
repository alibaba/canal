package com.alibaba.otter.canal.connector.core.producer;

/**
 * MQ producer destination
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class MQDestination {

    private String  canalDestination;
    private String  topic;
    private Integer partition;
    private Integer partitionsNum;
    private String  partitionHash;
    private String  dynamicTopic;

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

    public String getPartitionHash() {
        return partitionHash;
    }

    public void setPartitionHash(String partitionHash) {
        this.partitionHash = partitionHash;
    }

    public String getDynamicTopic() {
        return dynamicTopic;
    }

    public void setDynamicTopic(String dynamicTopic) {
        this.dynamicTopic = dynamicTopic;
    }
}

package com.alibaba.otter.canal.instance.core;

import java.util.LinkedHashMap;
import java.util.Map;

public class CanalMQConfig {

    private String                       topic;
    private Integer                      partition;
    private Integer                      partitionsNum;
    private String                       partitionHash;

    private volatile Map<String, String> partitionHashProperties;

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

    public Map<String, String> getPartitionHashProperties() {
        if (partitionHashProperties == null) {
            synchronized (CanalMQConfig.class) {
                if (partitionHashProperties == null) {
                    if (partitionHash != null) {
                        partitionHashProperties = new LinkedHashMap<>();
                        String[] items = partitionHash.split(",");
                        for (String item : items) {
                            int i = item.indexOf(":");
                            if (i > -1) {
                                String dbTable = item.substring(0, i).trim();
                                String pk = item.substring(i + 1).trim();
                                partitionHashProperties.put(dbTable, pk);
                            }
                        }
                    }
                }
            }
        }
        return partitionHashProperties;
    }
}

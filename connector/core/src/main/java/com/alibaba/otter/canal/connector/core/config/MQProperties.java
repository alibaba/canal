package com.alibaba.otter.canal.connector.core.config;

/**
 * MQ配置类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class MQProperties {

    private boolean flatMessage             = true;
    private boolean databaseHash            = true;
    private boolean filterTransactionEntry  = true;
    private Integer parallelBuildThreadSize = 8;
    private Integer parallelSendThreadSize  = 30;
    private Integer fetchTimeout            = 100;
    private Integer batchSize               = 50;
    private String  accessChannel           = "local";

    private String  aliyunAccessKey         = "";
    private String  aliyunSecretKey         = "";
    private int     aliyunUid               = 0;

    public boolean isFlatMessage() {
        return flatMessage;
    }

    public void setFlatMessage(boolean flatMessage) {
        this.flatMessage = flatMessage;
    }

    public boolean isDatabaseHash() {
        return databaseHash;
    }

    public void setDatabaseHash(boolean databaseHash) {
        this.databaseHash = databaseHash;
    }

    public boolean isFilterTransactionEntry() {
        return filterTransactionEntry;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public Integer getParallelBuildThreadSize() {
        return parallelBuildThreadSize;
    }

    public void setParallelBuildThreadSize(Integer parallelBuildThreadSize) {
        this.parallelBuildThreadSize = parallelBuildThreadSize;
    }

    public Integer getParallelSendThreadSize() {
        return parallelSendThreadSize;
    }

    public void setParallelSendThreadSize(Integer parallelSendThreadSize) {
        this.parallelSendThreadSize = parallelSendThreadSize;
    }

    public Integer getFetchTimeout() {
        return fetchTimeout;
    }

    public void setFetchTimeout(Integer fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(String accessChannel) {
        this.accessChannel = accessChannel;
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

    public int getAliyunUid() {
        return aliyunUid;
    }

    public void setAliyunUid(int aliyunUid) {
        this.aliyunUid = aliyunUid;
    }
}

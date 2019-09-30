package com.alibaba.otter.canal.common;

import java.util.Properties;

/**
 * MQ 配置项
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class MQProperties {

    private String     servers                = "127.0.0.1:6667";
    private int        retries                = 0;
    private int        batchSize              = 16384;
    private int        lingerMs               = 1;
    private int        maxRequestSize         = 1048576;
    private long       bufferMemory           = 33554432L;
    private boolean    filterTransactionEntry = true;
    private String     producerGroup          = "Canal-Producer";
    private int        canalBatchSize         = 50;
    private Long       canalGetTimeout        = 100L;
    private boolean    flatMessage            = true;
    private String     compressionType        = "none";
    private String     acks                   = "all";
    private String     aliyunAccessKey        = "";
    private String     aliyunSecretKey        = "";
    private Properties properties             = new Properties();
    private boolean    enableMessageTrace     = false;
    private String     accessChannel          = null;
    private String     customizedTraceTopic   = null;
    private String     namespace              = "";
    // kafka集群是否启动Kerberos认证
    private boolean    kerberosEnable         = false;
    // 启动Kerberos认证时配置为krb5.conf文件的路径
    private String     kerberosKrb5FilePath   = "";
    // 启动Kerberos认证时配置为jaas.conf文件的路径
    private String     kerberosJaasFilePath   = "";
    // rabbitmq 账号
    private String     username               = "";
    // rabbitmq 密码
    private String     password               = "";
    // rabbitmq 密码
    private String     vhost                  = "";
    // aliyun 用户ID rabbitmq 阿里云需要使用
    private long       aliyunUID              = 0;
    // rabbitmq 交换机
    private String     exchange               = "";
    // 消息发送的并行度
    private int        parallelThreadSize     = 8;

    public static class CanalDestination {

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

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public boolean isEnableMessageTrace() {
        return enableMessageTrace;
    }

    public void setEnableMessageTrace(boolean enableMessageTrace) {
        this.enableMessageTrace = enableMessageTrace;
    }

    public String getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(String accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getCustomizedTraceTopic() {
        return customizedTraceTopic;
    }

    public void setCustomizedTraceTopic(String customizedTraceTopic) {
        this.customizedTraceTopic = customizedTraceTopic;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public boolean isKerberosEnable() {
        return kerberosEnable;
    }

    public void setKerberosEnable(boolean kerberosEnable) {
        this.kerberosEnable = kerberosEnable;
    }

    public String getKerberosKrb5FilePath() {
        return kerberosKrb5FilePath;
    }

    public void setKerberosKrb5FilePath(String kerberosKrb5FilePath) {
        this.kerberosKrb5FilePath = kerberosKrb5FilePath;
    }

    public String getKerberosJaasFilePath() {
        return kerberosJaasFilePath;
    }

    public void setKerberosJaasFilePath(String kerberosJaasFilePath) {
        this.kerberosJaasFilePath = kerberosJaasFilePath;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setVhost(String vhost) {
        this.vhost = vhost;
    }

    public String getVhost() {
        return vhost;
    }

    public long getAliyunUID() {
        return aliyunUID;
    }

    public void setAliyunUID(long aliyunUID) {
        this.aliyunUID = aliyunUID;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public int getParallelThreadSize() {
        return parallelThreadSize;
    }

    public void setParallelThreadSize(int parallelThreadSize) {
        this.parallelThreadSize = parallelThreadSize;
    }

    @Override
    public String toString() {
        return "MQProperties [servers=" + servers + ", retries=" + retries + ", batchSize=" + batchSize + ", lingerMs="
               + lingerMs + ", maxRequestSize=" + maxRequestSize + ", bufferMemory=" + bufferMemory
               + ", filterTransactionEntry=" + filterTransactionEntry + ", producerGroup=" + producerGroup
               + ", canalBatchSize=" + canalBatchSize + ", canalGetTimeout=" + canalGetTimeout + ", flatMessage="
               + flatMessage + ", compressionType=" + compressionType + ", acks=" + acks + ", aliyunAccessKey="
               + aliyunAccessKey + ", aliyunSecretKey=" + aliyunSecretKey + ", properties=" + properties
               + ", enableMessageTrace=" + enableMessageTrace + ", accessChannel=" + accessChannel
               + ", customizedTraceTopic=" + customizedTraceTopic + ", namespace=" + namespace + ", kerberosEnable="
               + kerberosEnable + ", kerberosKrb5FilePath=" + kerberosKrb5FilePath + ", kerberosJaasFilePath="
               + kerberosJaasFilePath + ", username=" + username + ", password=" + password + ", vhost=" + vhost
               + ", aliyunUID=" + aliyunUID + ", exchange=" + exchange + ", parallelThreadSize=" + parallelThreadSize
               + "]";
    }

}

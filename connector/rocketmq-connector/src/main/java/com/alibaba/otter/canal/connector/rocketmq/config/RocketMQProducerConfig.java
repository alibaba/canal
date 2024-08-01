package com.alibaba.otter.canal.connector.rocketmq.config;

import com.alibaba.otter.canal.connector.core.config.MQProperties;

/**
 * RocketMQ 配置类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.0
 */
public class RocketMQProducerConfig extends MQProperties {

    private String  producerGroup;
    private boolean enableMessageTrace       = false;
    private String  customizedTraceTopic;
    private String  namespace;
    private String  namesrvAddr;
    private int     retryTimesWhenSendFailed = 0;
    private boolean vipChannelEnabled        = false;
    private String  tag;

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public boolean isEnableMessageTrace() {
        return enableMessageTrace;
    }

    public void setEnableMessageTrace(boolean enableMessageTrace) {
        this.enableMessageTrace = enableMessageTrace;
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

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}

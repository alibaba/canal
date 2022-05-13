package com.alibaba.otter.canal.connector.pulsarmq.config;

import com.alibaba.otter.canal.connector.core.config.MQProperties;

/**
 * Pulsar生产者配置
 * 
 * @author chad
 * @date 2021/9/15 11:23
 * @since 1 by chad at 2021/9/15 新增
 */
public class PulsarMQProducerConfig extends MQProperties {

    /**
     * pulsar服务连接地址
     * <p>
     * broker: pulsar://localhost:6650<br/>
     * httpUrl: http://localhost:8080
     * </p>
     */
    private String serverUrl;
    /**
     * pulsar topic前缀
     * <p>
     * 正常的pulsar topic全路径为：persistent://{tenant}/{namespace}/{TOPIC}，
     * 而为了方便，在配置文件中仅需要配置 {TOPIC} 段即可，persistent://{tenant}/{namespace}由此参数控制。
     * 在发送消息时会自动拼接上
     * </p>
     */
    private String topicTenantPrefix;
    /**
     * 生产者角色权限，请确保该角色有canal使用的所有topic生产者权限（最低要求）
     */
    private String roleToken;
    /**
     * admin服务器地址
     */
    private String adminServerUrl;

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getRoleToken() {
        return roleToken;
    }

    public void setRoleToken(String roleToken) {
        this.roleToken = roleToken;
    }

    public String getTopicTenantPrefix() {
        return topicTenantPrefix;
    }

    public void setTopicTenantPrefix(String topicTenantPrefix) {
        this.topicTenantPrefix = topicTenantPrefix;
    }

    public String getAdminServerUrl() {
        return adminServerUrl;
    }

    public void setAdminServerUrl(String adminServerUrl) {
        this.adminServerUrl = adminServerUrl;
    }
}

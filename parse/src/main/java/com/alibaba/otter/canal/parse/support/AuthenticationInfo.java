package com.alibaba.otter.canal.parse.support;

import java.net.InetSocketAddress;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * 数据库认证信息
 * 
 * @author jianghang 2012-7-11 上午11:22:19
 * @version 4.1.0
 */
public class AuthenticationInfo {

    private InetSocketAddress address;            // 主库信息
    private String            username;           // 帐号
    private String            password;           // 密码
    private String            defaultDatabaseName; // 默认链接的数据库

    public AuthenticationInfo(){
        super();
    }

    public AuthenticationInfo(InetSocketAddress address, String username, String password){
        this(address, username, password, "");
    }

    public AuthenticationInfo(InetSocketAddress address, String username, String password, String defaultDatabaseName){
        this.address = address;
        this.username = username;
        this.password = password;
        this.defaultDatabaseName = defaultDatabaseName;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDefaultDatabaseName() {
        return defaultDatabaseName;
    }

    public void setDefaultDatabaseName(String defaultDatabaseName) {
        this.defaultDatabaseName = defaultDatabaseName;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

}

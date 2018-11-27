package com.alibaba.otter.canal.parse.support;

import java.net.InetSocketAddress;

import com.alibaba.druid.filter.config.ConfigTools;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * 数据库认证信息
 *
 * @author jianghang 2012-7-11 上午11:22:19
 * @version 1.0.0
 */
public class AuthenticationInfo {



    private InetSocketAddress address;            // 主库信息
    private String            username;           // 帐号
    private String            password;           // 密码
    private String            defaultDatabaseName;// 默认链接的数据库
    private String            pwdPublicKey;       //公钥
    private boolean           enableDruid;        //是否使用druid加密解密数据库密码

    public void initPwd() throws Exception{
        if (enableDruid) {
            this.password = ConfigTools.decrypt(pwdPublicKey, password);
        }
    }

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

    public String getPwdPublicKey() {
        return pwdPublicKey;
    }

    public void setPwdPublicKey(String pwdPublicKey) {
        this.pwdPublicKey = pwdPublicKey;
    }

    public boolean isEnableDruid() {
        return enableDruid;
    }

    public void setEnableDruid(boolean enableDruid) {
        this.enableDruid = enableDruid;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result + ((defaultDatabaseName == null) ? 0 : defaultDatabaseName.hashCode());
        result = prime * result + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AuthenticationInfo)) {
            return false;
        }
        AuthenticationInfo other = (AuthenticationInfo) obj;
        if (address == null) {
            if (other.address != null) {
                return false;
            }
        } else if (!address.equals(other.address)) {
            return false;
        }
        if (defaultDatabaseName == null) {
            if (other.defaultDatabaseName != null) {
                return false;
            }
        } else if (!defaultDatabaseName.equals(other.defaultDatabaseName)) {
            return false;
        }
        if (password == null) {
            if (other.password != null) {
                return false;
            }
        } else if (!password.equals(other.password)) {
            return false;
        }
        if (username == null) {
            if (other.username != null) {
                return false;
            }
        } else if (!username.equals(other.username)) {
            return false;
        }
        return true;
    }

}

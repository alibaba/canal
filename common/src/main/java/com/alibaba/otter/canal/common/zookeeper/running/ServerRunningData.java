package com.alibaba.otter.canal.common.zookeeper.running;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 服务端running状态信息
 * 
 * @author jianghang 2012-11-22 下午03:11:30
 * @version 1.0.0
 */
public class ServerRunningData implements Serializable {

    private static final long serialVersionUID = 92260481691855281L;

    @Deprecated
    private Long              cid;
    private String            address;
    private boolean           active           = true;

    public ServerRunningData(){
    }

    public ServerRunningData(String address){
        this.address = address;
    }

    public Long getCid() {
        return cid;
    }

    public void setCid(Long cid) {
        this.cid = cid;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}

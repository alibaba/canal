package com.alibaba.otter.canal.protocol.position;

import java.net.InetSocketAddress;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * @author jianghang 2012-6-21 上午10:52:02
 * @version 1.0.0
 */
public class LogIdentity extends Position {

    private static final long serialVersionUID = 5530225131455662581L;
    private InetSocketAddress sourceAddress;                          // 链接服务器的地址
    private Long              slaveId;                                // 对应的slaveId

    public LogIdentity(){
    }

    public LogIdentity(InetSocketAddress sourceAddress, Long slaveId){
        this.sourceAddress = sourceAddress;
        this.slaveId = slaveId;
    }

    public InetSocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public void setSourceAddress(InetSocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public Long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(Long slaveId) {
        this.slaveId = slaveId;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((slaveId == null) ? 0 : slaveId.hashCode());
        result = prime * result + ((sourceAddress == null) ? 0 : sourceAddress.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        LogIdentity other = (LogIdentity) obj;
        if (slaveId == null) {
            if (other.slaveId != null) return false;
        } else if (slaveId.longValue() != (other.slaveId.longValue())) return false;
        if (sourceAddress == null) {
            if (other.sourceAddress != null) return false;
        } else if (!sourceAddress.equals(other.sourceAddress)) return false;
        return true;
    }

}

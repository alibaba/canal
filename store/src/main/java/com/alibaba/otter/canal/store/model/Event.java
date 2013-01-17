package com.alibaba.otter.canal.store.model;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.LogIdentity;

/**
 * store存储数据对象
 * 
 * @author jianghang 2012-7-13 下午03:03:03
 */
public class Event implements Serializable {

    private static final long serialVersionUID = 1333330351758762739L;

    private LogIdentity       logIdentity;                            // 记录数据产生的来源
    private CanalEntry.Entry  entry;

    public Event(){
    }

    public Event(LogIdentity logIdentity, CanalEntry.Entry entry){
        this.logIdentity = logIdentity;
        this.entry = entry;
    }

    public LogIdentity getLogIdentity() {
        return logIdentity;
    }

    public void setLogIdentity(LogIdentity logIdentity) {
        this.logIdentity = logIdentity;
    }

    public CanalEntry.Entry getEntry() {
        return entry;
    }

    public void setEntry(CanalEntry.Entry entry) {
        this.entry = entry;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}

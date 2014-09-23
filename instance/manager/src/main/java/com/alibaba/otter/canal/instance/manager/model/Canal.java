package com.alibaba.otter.canal.instance.manager.model;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 对应的canal模型对象
 * 
 * @author jianghang 2012-7-4 下午02:32:39
 * @version 1.0.0
 */
public class Canal implements Serializable {

    private static final long serialVersionUID = 8333284022624682754L;

    private Long              id;
    private String            name;                                   // 对应的名字
    private String            desc;                                   // 描述
    private CanalStatus       status;
    private CanalParameter    canalParameter;                         // 参数定义
    private Date              gmtCreate;                              // 创建时间
    private Date              gmtModified;                            // 修改时间

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public CanalParameter getCanalParameter() {
        return canalParameter;
    }

    public void setCanalParameter(CanalParameter canalParameter) {
        this.canalParameter = canalParameter;
    }

    public CanalStatus getStatus() {
        return status;
    }

    public void setStatus(CanalStatus status) {
        this.status = status;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}

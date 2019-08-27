package com.alibaba.otter.canal.admin.model;

import io.ebean.Finder;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;

/**
 * Canal实例配置信息实体类
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Entity
public class CanalInstanceConfig extends Model {

    public static final CanalInstanceConfigFinder find = new CanalInstanceConfigFinder();

    public static class CanalInstanceConfigFinder extends Finder<Long, CanalInstanceConfig> {

        /**
         * Construct using the default EbeanServer.
         */
        public CanalInstanceConfigFinder(){
            super(CanalInstanceConfig.class);
        }

    }

    @Id
    private Long   id;
    private String name;
    private String content;
    private String status;
    private Date   modifiedTime;

    @Transient
    private Long   nodeId;
    @Transient
    private String nodeIp;

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

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(Date modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }
}

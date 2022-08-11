package com.alibaba.otter.canal.admin.model;

import io.ebean.Finder;
import io.ebean.annotation.WhenModified;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Canal主配置实体类
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Entity
public class CanalConfig extends Model {

    public static final CanalConfigFinder find = new CanalConfigFinder();

    public static class CanalConfigFinder extends Finder<Long, CanalConfig> {

        /**
         * Construct using the default EbeanServer.
         */
        public CanalConfigFinder(){
            super(CanalConfig.class);
        }

    }

    @Id
    private Long   id;
    private Long   clusterId;
    private Long   serverId;
    private String name;
    private String content;
    private String contentMd5;
    private String status;
    @WhenModified
    private Date   modifiedTime;

    public void init() {
        this.name = "canal.properties";
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getClusterId() {
        return clusterId;
    }

    public void setClusterId(Long clusterId) {
        this.clusterId = clusterId;
    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
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

    public String getContentMd5() {
        return contentMd5;
    }

    public void setContentMd5(String contentMd5) {
        this.contentMd5 = contentMd5;
    }

    public Date getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(Date modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

}

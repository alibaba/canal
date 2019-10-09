package com.alibaba.otter.canal.admin.model;

import java.util.Date;

import javax.persistence.*;

import io.ebean.Finder;
import io.ebean.annotation.WhenModified;

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
    private Long         id;
    @Column(name = "cluster_id")
    private Long         clusterId;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id", updatable = false, insertable = false)
    private CanalCluster canalCluster;
    @Column(name = "server_id")
    private Long         serverId;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "server_id", updatable = false, insertable = false)
    private NodeServer   nodeServer;
    private String       name;
    private String       content;
    private String       contentMd5;
    private String       status;         // 1: 正常 0: 停止
    @WhenModified
    private Date         modifiedTime;

    @Transient
    private String       clusterServerId;
    @Transient
    private String       runningStatus = "0";  // 1: 运行中 0: 停止

    public void init() {
        status = "1";
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

    public CanalCluster getCanalCluster() {
        return canalCluster;
    }

    public void setCanalCluster(CanalCluster canalCluster) {
        this.canalCluster = canalCluster;
    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
    }

    public NodeServer getNodeServer() {
        return nodeServer;
    }

    public void setNodeServer(NodeServer nodeServer) {
        this.nodeServer = nodeServer;
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

    public String getClusterServerId() {
        return clusterServerId;
    }

    public void setClusterServerId(String clusterServerId) {
        this.clusterServerId = clusterServerId;
    }

    public String getRunningStatus() {
        return runningStatus;
    }

    public void setRunningStatus(String runningStatus) {
        this.runningStatus = runningStatus;
    }
}

package com.alibaba.otter.canal.admin.model;

import io.ebean.Finder;
import io.ebean.annotation.WhenModified;

import java.util.Date;

import javax.persistence.*;

/**
 * 节点信息实体类
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Entity
@Table(name = "canal_node_server")
public class NodeServer extends Model {

    public static final NodeServerFinder find = new NodeServerFinder();

    public static class NodeServerFinder extends Finder<Long, NodeServer> {

        /**
         * Construct using the default EbeanServer.
         */
        public NodeServerFinder(){
            super(NodeServer.class);
        }

    }

    @Id
    private Long         id;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id", updatable = false, insertable = false)
    private CanalCluster canalCluster;
    @Column(name = "cluster_id")
    private Long         clusterId;
    private String       name;
    private String       ip;
    private Integer      adminPort;
    private Integer      metricPort;
    private Integer      tcpPort;
    private String       status;
    @WhenModified
    private Date         modifiedTime;

    public void init() {
        status = "-1";
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public CanalCluster getCanalCluster() {
        return canalCluster;
    }

    public void setCanalCluster(CanalCluster canalCluster) {
        this.canalCluster = canalCluster;
    }

    public Long getClusterId() {
        return clusterId;
    }

    public void setClusterId(Long clusterId) {
        this.clusterId = clusterId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getAdminPort() {
        return adminPort;
    }

    public void setAdminPort(Integer adminPort) {
        this.adminPort = adminPort;
    }

    public Integer getMetricPort() {
        return metricPort;
    }

    public void setMetricPort(Integer metricPort) {
        this.metricPort = metricPort;
    }

    public Integer getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(Integer tcpPort) {
        this.tcpPort = tcpPort;
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
}

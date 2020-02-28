package com.alibaba.otter.canal.admin.model;

import io.ebean.Finder;
import io.ebean.annotation.WhenModified;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Canal集群信息实体类
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Entity
@Table(name = "canal_cluster")
public class CanalCluster extends Model {

    public static final CanalClusterFinder find = new CanalClusterFinder();

    public static class CanalClusterFinder extends Finder<Long, CanalCluster> {

        /**
         * Construct using the default EbeanServer.
         */
        public CanalClusterFinder(){
            super(CanalCluster.class);
        }

    }

    @Id
    private Long   id;
    private String name;
    private String zkHosts;
    @WhenModified
    private Date   modifiedTime;

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

    public String getZkHosts() {
        return zkHosts;
    }

    public void setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
    }

    public Date getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(Date modifiedTime) {
        this.modifiedTime = modifiedTime;
    }
}

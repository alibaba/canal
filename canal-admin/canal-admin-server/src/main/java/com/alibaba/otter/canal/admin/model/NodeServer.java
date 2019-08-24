package com.alibaba.otter.canal.admin.model;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import io.ebean.Finder;

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
    private Long    id;
    private String  name;
    private String  ip;
    private Integer port;
    private Integer port2;
    private Integer status;
    private Date    modifiedTime;

    public void init() {
        status = -1;
    }

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

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getPort2() {
        return port2;
    }

    public void setPort2(Integer port2) {
        this.port2 = port2;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Date getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(Date modifiedTime) {
        this.modifiedTime = modifiedTime;
    }
}

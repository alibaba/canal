package com.alibaba.otter.canal.admin.model;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;

import io.ebean.Finder;

@Entity
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
    private Integer status = -1;
    private Date    modifiedTime;

    public void init() {

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

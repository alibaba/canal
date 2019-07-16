package com.alibaba.otter.canal.admin.model;

import io.ebean.Finder;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Canal住配置实体类
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
    private String name;
    private String content;
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

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Date getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(Date modifiedTime) {
        this.modifiedTime = modifiedTime;
    }
}

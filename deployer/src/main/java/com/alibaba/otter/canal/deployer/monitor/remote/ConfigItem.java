package com.alibaba.otter.canal.deployer.monitor.remote;

/**
 * 配置对应对象
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public class ConfigItem {

    private Long   id;
    private String name;
    private String content;
    private long   modifiedTime;

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

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }
}

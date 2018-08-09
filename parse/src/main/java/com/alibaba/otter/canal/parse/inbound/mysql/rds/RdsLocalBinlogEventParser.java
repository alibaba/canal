package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import java.io.File;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinlogEventParser;
import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * 基于rds binlog备份文件的复制
 * 
 * @author agapple 2017年10月15日 下午1:27:36
 * @since 1.0.25
 */
public class RdsLocalBinlogEventParser extends LocalBinlogEventParser implements CanalEventParser {

    private String url = "https://rds.aliyuncs.com/"; // openapi地址
    private String accesskey;                        // 云账号的ak
    private String secretkey;                        // 云账号sk
    private String instanceId;                       // rds实例id
    private Long   startTime;
    private Long   endTime;

    public RdsLocalBinlogEventParser(){
    }

    public void start() throws CanalParseException {
        try {
            Assert.notNull(startTime);
            Assert.notNull(accesskey);
            Assert.notNull(secretkey);
            Assert.notNull(instanceId);
            Assert.notNull(url);
            if (endTime == null) {
                endTime = System.currentTimeMillis();
            }

            RdsBinlogOpenApi.downloadBinlogFiles(url,
                accesskey,
                secretkey,
                instanceId,
                new Date(startTime),
                new Date(endTime),
                new File(directory));

            // 更新一下时间戳
            masterPosition = new EntryPosition(startTime);
        } catch (Throwable e) {
            logger.error("download binlog failed", e);
            throw new CanalParseException(e);
        }

        super.start();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        if (StringUtils.isNotEmpty(url)) {
            this.url = url;
        }
    }

    public String getAccesskey() {
        return accesskey;
    }

    public void setAccesskey(String accesskey) {
        this.accesskey = accesskey;
    }

    public String getSecretkey() {
        return secretkey;
    }

    public void setSecretkey(String secretkey) {
        this.secretkey = secretkey;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

}

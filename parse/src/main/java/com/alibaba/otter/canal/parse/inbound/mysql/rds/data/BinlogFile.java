package com.alibaba.otter.canal.parse.inbound.mysql.rds.data;

/**
 * @author chengjin.lyf on 2018/8/7 下午2:26
 * @since 1.0.25
 */
public class BinlogFile {

    private Long   FileSize;
    private String LogBeginTime;
    private String LogEndTime;
    private String DownloadLink;
    private String HostInstanceID;
    private String LinkExpiredTime;
    private String fileName;

    public Long getFileSize() {
        return FileSize;
    }

    public void setFileSize(Long fileSize) {
        FileSize = fileSize;
    }

    public String getLogBeginTime() {
        return LogBeginTime;
    }

    public void setLogBeginTime(String logBeginTime) {
        LogBeginTime = logBeginTime;
    }

    public String getLogEndTime() {
        return LogEndTime;
    }

    public void setLogEndTime(String logEndTime) {
        LogEndTime = logEndTime;
    }

    public String getDownloadLink() {
        return DownloadLink;
    }

    public void setDownloadLink(String downloadLink) {
        DownloadLink = downloadLink;
    }

    public String getHostInstanceID() {
        return HostInstanceID;
    }

    public void setHostInstanceID(String hostInstanceID) {
        HostInstanceID = hostInstanceID;
    }

    public String getLinkExpiredTime() {
        return LinkExpiredTime;
    }

    public void setLinkExpiredTime(String linkExpiredTime) {
        LinkExpiredTime = linkExpiredTime;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public String toString() {
        return "BinlogFile [FileSize=" + FileSize + ", LogBeginTime=" + LogBeginTime + ", LogEndTime=" + LogEndTime
               + ", DownloadLink=" + DownloadLink + ", HostInstanceID=" + HostInstanceID + ", LinkExpiredTime="
               + LinkExpiredTime + ", fileName=" + fileName + "]";
    }

}

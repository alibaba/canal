package com.alibaba.otter.canal.parse.inbound.mysql.rds.data;

/**
 * @author chengjin.lyf on 2018/8/7 下午2:26
 * @since 1.0.25
 */
public class RdsBackupPolicy {

    /**
     * 数据备份保留天数（7到730天）。
     */
    private String  BackupRetentionPeriod;
    /**
     * 数据备份时间，格式：HH:mmZ- HH:mm Z。
     */
    private String  PreferredBackupTime;
    /**
     * 数据备份周期。Monday：周一；Tuesday：周二；Wednesday：周三；Thursday：周四；Friday：周五；Saturday：
     * 周六；Sunday：周日。
     */
    private String  PreferredBackupPeriod;
    /**
     * 日志备份状态。Enable：开启；Disabled：关闭。
     */
    private boolean BackupLog;
    /**
     * 日志备份保留天数（7到730天）。
     */
    private int     LogBackupRetentionPeriod;

    public String getBackupRetentionPeriod() {
        return BackupRetentionPeriod;
    }

    public void setBackupRetentionPeriod(String backupRetentionPeriod) {
        BackupRetentionPeriod = backupRetentionPeriod;
    }

    public String getPreferredBackupTime() {
        return PreferredBackupTime;
    }

    public void setPreferredBackupTime(String preferredBackupTime) {
        PreferredBackupTime = preferredBackupTime;
    }

    public String getPreferredBackupPeriod() {
        return PreferredBackupPeriod;
    }

    public void setPreferredBackupPeriod(String preferredBackupPeriod) {
        PreferredBackupPeriod = preferredBackupPeriod;
    }

    public boolean isBackupLog() {
        return BackupLog;
    }

    public void setBackupLog(boolean backupLog) {
        BackupLog = backupLog;
    }

    public int getLogBackupRetentionPeriod() {
        return LogBackupRetentionPeriod;
    }

    public void setLogBackupRetentionPeriod(int logBackupRetentionPeriod) {
        LogBackupRetentionPeriod = logBackupRetentionPeriod;
    }

    @Override
    public String toString() {
        return "RdsBackupPolicy [BackupRetentionPeriod=" + BackupRetentionPeriod + ", PreferredBackupTime="
               + PreferredBackupTime + ", PreferredBackupPeriod=" + PreferredBackupPeriod + ", BackupLog=" + BackupLog
               + ", LogBackupRetentionPeriod=" + LogBackupRetentionPeriod + "]";
    }

}

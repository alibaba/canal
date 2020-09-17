package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import java.io.File;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.PositionNotFoundException;
import com.alibaba.otter.canal.parse.exception.ServerIdNotMatchException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinLogConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.LocalBinlogEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * 基于rds binlog备份文件的复制
 * 
 * @author agapple 2017年10月15日 下午1:27:36
 * @since 1.0.25
 */
public class RdsLocalBinlogEventParser extends LocalBinlogEventParser implements CanalEventParser, LocalBinLogConnection.FileParserListener {

    private String              url;                // openapi地址
    private String              accesskey;          // 云账号的ak
    private String              secretkey;          // 云账号sk
    private String              instanceId;         // rds实例id
    private Long                startTime;
    private Long                endTime;
    private BinlogDownloadQueue binlogDownloadQueue;
    private ParseFinishListener finishListener;
    private int                 batchFileSize;

    public RdsLocalBinlogEventParser(){
    }

    public void start() throws CanalParseException {
        try {
            Assert.notNull(accesskey);
            Assert.notNull(secretkey);
            Assert.notNull(instanceId);
            Assert.notNull(url);
            Assert.notNull(directory);

            if (endTime == null) {
                endTime = System.currentTimeMillis();
            }

            EntryPosition entryPosition = findStartPosition(null);
            if (entryPosition == null) {
                throw new PositionNotFoundException("position not found!");
            }
            Long startTimeInMill = entryPosition.getTimestamp();
            if (startTimeInMill == null || startTimeInMill <= 0) {
                throw new PositionNotFoundException("position timestamp is empty!");
            }

            startTime = startTimeInMill;
            List<BinlogFile> binlogFiles = RdsBinlogOpenApi.listBinlogFiles(url,
                accesskey,
                secretkey,
                instanceId,
                new Date(startTime),
                new Date(endTime));
            if (binlogFiles.isEmpty()) {
                throw new CanalParseException("start timestamp : " + startTimeInMill + " binlog files is empty");
            }

            binlogDownloadQueue = new BinlogDownloadQueue(binlogFiles, batchFileSize, directory);
            binlogDownloadQueue.silenceDownload();
            needWait = true;
            // try to download one file,use to test server id
            binlogDownloadQueue.tryOne();
        } catch (Throwable e) {
            logger.error("download binlog failed", e);
            throw new CanalParseException(e);
        }
        setParserExceptionHandler(this::handleMysqlParserException);
        super.start();
    }

    private void handleMysqlParserException(Throwable throwable) {
        if (throwable instanceof ServerIdNotMatchException) {
            logger.error("server id not match, try download another rds binlog!");
            binlogDownloadQueue.notifyNotMatch();
            try {
                binlogDownloadQueue.cleanDir();
                binlogDownloadQueue.tryOne();
                binlogDownloadQueue.prepare();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

            try {
                binlogDownloadQueue.execute(() -> {
                    RdsLocalBinlogEventParser.super.stop();
                    RdsLocalBinlogEventParser.super.start();
                });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        ErosaConnection connection = super.buildErosaConnection();
        if (connection instanceof LocalBinLogConnection) {
            LocalBinLogConnection localBinLogConnection = (LocalBinLogConnection) connection;
            localBinLogConnection.setNeedWait(true);
            localBinLogConnection.setServerId(serverId);
            localBinLogConnection.setParserListener(this);
        }
        return connection;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        if (StringUtils.isNotEmpty(url)) {
            this.url = url;
        }
    }

    public void setAccesskey(String accesskey) {
        this.accesskey = accesskey;
    }

    public void setSecretkey(String secretkey) {
        this.secretkey = secretkey;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    @Override
    public void onFinish(String fileName) {
        try {
            binlogDownloadQueue.downOne();
            File needDeleteFile = new File(directory + File.separator + fileName);
            if (needDeleteFile.exists()) {
                needDeleteFile.delete();
            }
            // 处理下logManager位点问题
            LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
            Long timestamp = 0L;
            if (logPosition != null && logPosition.getPostion() != null) {
                timestamp = logPosition.getPostion().getTimestamp();
                EntryPosition position = logPosition.getPostion();
                LogPosition newLogPosition = new LogPosition();
                String journalName = position.getJournalName();
                int sepIdx = journalName.indexOf(".");
                String fileIndex = journalName.substring(sepIdx + 1);
                int index = NumberUtils.toInt(fileIndex) + 1;
                String newJournalName = journalName.substring(0, sepIdx) + "."
                                        + StringUtils.leftPad(String.valueOf(index), fileIndex.length(), "0");
                newLogPosition.setPostion(new EntryPosition(newJournalName,
                    4L,
                    position.getTimestamp(),
                    position.getServerId()));
                newLogPosition.setIdentity(logPosition.getIdentity());
                logPositionManager.persistLogPosition(destination, newLogPosition);
            }

            if (binlogDownloadQueue.isLastFile(fileName)) {
                logger.warn("last file : " + fileName + " , timestamp : " + timestamp
                            + " , all file parse complete, switch to mysql parser!");
                finishListener.onFinish();
                return;
            } else {
                logger.warn("parse local binlog file : " + fileName + " , timestamp : " + timestamp
                            + " , try the next binlog !");
            }
            binlogDownloadQueue.prepare();
        } catch (Exception e) {
            logger.error("prepare download binlog file failed!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        this.binlogDownloadQueue.release();
        super.stop();
    }

    public void setFinishListener(ParseFinishListener finishListener) {
        this.finishListener = finishListener;
    }

    public interface ParseFinishListener {

        void onFinish();
    }

    public void setBatchFileSize(int batchFileSize) {
        this.batchFileSize = batchFileSize;
    }
}

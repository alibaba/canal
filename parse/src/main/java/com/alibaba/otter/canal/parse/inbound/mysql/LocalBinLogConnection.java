package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.erosa.parse.events.QueryLogEvent;
import com.alibaba.erosa.parse.utils.MysqlBinlogConstants;
import com.alibaba.erosa.parse.utils.MysqlBinlogEventUtil;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.local.BinLogFileQueue;
import com.alibaba.otter.canal.parse.inbound.mysql.local.BufferedFileDataInput;

/**
 * local bin log connection (not real connection)
 * 
 * @author yuanzu Date: 12-9-27 Time: 下午6:14
 */
public class LocalBinLogConnection implements ErosaConnection {

    private static final Logger logger                 = LoggerFactory.getLogger(LocalBinLogConnection.class);
    private static final long   BINLOG_START_OFFEST    = 4L;
    private static final int    EVENT_TIMESTAMP_OFFSET = 0;
    private BinLogFileQueue     binlogs                = null;
    private boolean             needWait;
    private String              directory;
    private int                 bufferSize             = 16 * 1024;
    private boolean             running                = false;

    public LocalBinLogConnection(){
    }

    public LocalBinLogConnection(String directory, boolean needWait){
        this.needWait = needWait;
        this.directory = directory;
    }

    @Override
    public void connect() throws IOException {
        if (this.binlogs == null) {
            this.binlogs = new BinLogFileQueue(this.directory);
        }
        this.running = true;
    }

    @Override
    public void reconnect() throws IOException {
        disconnect();
        connect();
    }

    @Override
    public void disconnect() throws IOException {
        this.running = false;
        if (this.binlogs != null) {
            this.binlogs.destory();
        }
        this.binlogs = null;
        this.running = false;
    }

    public boolean isConnected() {
        return running;
    }

    @Override
    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        File current = new File(directory, binlogfilename);
        BufferedFileDataInput input = null;

        try {
            input = new BufferedFileDataInput(current, bufferSize);
            input.seek(binlogPosition);

            while (running) {
                boolean needContinue = true;
                while (input.available() > 0 && needContinue) {
                    byte[] header = new byte[MysqlBinlogConstants.LOG_EVENT_HEADER_LEN];
                    input.readFully(header);
                    if (header.length < MysqlBinlogConstants.LOG_EVENT_HEADER_LEN) {
                        logger.warn("unexpected packet length on body with header bytes:{}", Arrays.toString(header));
                    }

                    int eventLength = (int) MysqlBinlogEventUtil.convertUnsigned4BytesToLong(
                                                                                             header,
                                                                                             MysqlBinlogConstants.EVENT_LEN_OFFSET);
                    eventLength -= header.length;
                    byte[] fullEvent = new byte[header.length + eventLength];
                    input.readFully(fullEvent, header.length, eventLength);
                    System.arraycopy(header, 0, fullEvent, 0, header.length);
                    // 判断一下是否还有数据
                    needContinue = func.sink(fullEvent);
                }

                if (needContinue) {// 读取下一个
                    input.close(); // 关闭上一个文件

                    File nextFile;
                    if (needWait) {
                        nextFile = binlogs.waitForNextFile(current);
                    } else {
                        nextFile = binlogs.getNextFile(current);
                    }

                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    input = new BufferedFileDataInput(current, bufferSize);
                    input.seek(BINLOG_START_OFFEST);
                } else {
                    break;// 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinLogConnection dump interrupted");
        } finally {
            if (input != null) {
                input.close();
            }
        }
    }

    @Override
    public void dump(long timestampMills, SinkFunction func) throws IOException {
        List<File> currentBinlogs = binlogs.currentBinlogs();
        File current = currentBinlogs.get(currentBinlogs.size() - 1);
        BufferedFileDataInput input = null;
        long timestampSeconds = timestampMills / 1000;

        String binlogFilename = null;
        long binlogFileOffset = 0;

        try {
            input = new BufferedFileDataInput(current, bufferSize);
            input.seek(BINLOG_START_OFFEST);
            long currentOffset = BINLOG_START_OFFEST;

            while (running) {
                boolean needContinue = true;
                String lastXidLogFilename = current.getName();
                long lastXidLogFileOffset = BINLOG_START_OFFEST;

                while (input.available() > 0 && needContinue) {
                    byte[] header = new byte[MysqlBinlogConstants.LOG_EVENT_HEADER_LEN];
                    input.readFully(header);
                    if (header.length < MysqlBinlogConstants.LOG_EVENT_HEADER_LEN) {
                        logger.warn("unexpected packet length on body with header bytes:{}", Arrays.toString(header));
                    }

                    long eventTimestamp = MysqlBinlogEventUtil.convertUnsigned4BytesToLong(header,
                                                                                           EVENT_TIMESTAMP_OFFSET);

                    int eventLength = (int) MysqlBinlogEventUtil.convertUnsigned4BytesToLong(
                                                                                             header,
                                                                                             MysqlBinlogConstants.EVENT_LEN_OFFSET);

                    int typeCode = MysqlBinlogEventUtil.convertUnsignedByteToInt(header,
                                                                                 MysqlBinlogConstants.EVENT_TYPE_OFFSET);

                    eventLength -= header.length;
                    byte[] fullEvent = new byte[header.length + eventLength];
                    input.readFully(fullEvent, header.length, eventLength);
                    System.arraycopy(header, 0, fullEvent, 0, header.length);

                    if (timestampSeconds < eventTimestamp) {
                        // process prev file
                        break;
                    }

                    currentOffset += fullEvent.length;

                    if (typeCode == MysqlBinlogConstants.QUERY_EVENT) {
                        QueryLogEvent event = new QueryLogEvent();
                        event.fromBytes(fullEvent);
                        if (StringUtils.endsWithIgnoreCase(event.query, "BEGIN")) {
                            binlogFilename = lastXidLogFilename;
                            binlogFileOffset = lastXidLogFileOffset;
                        }
                    } else if (typeCode == MysqlBinlogConstants.XID_EVENT) {
                        lastXidLogFilename = current.getName();
                        lastXidLogFileOffset = currentOffset;
                    }
                }

                if (needContinue) {// 读取下一个
                    input.close(); // 关闭上一个文件

                    File nextFile;

                    nextFile = binlogs.getBefore(current);

                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    input = new BufferedFileDataInput(current, bufferSize);
                    input.seek(BINLOG_START_OFFEST);
                } else {
                    break;// 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinLogConnection dump interrupted");
        } finally {
            if (input != null) {
                input.close();
            }
        }

        dump(binlogFilename, binlogFileOffset, func);
    }

    @Override
    public ErosaConnection fork() {
        LocalBinLogConnection connection = new LocalBinLogConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        connection.setNeedWait(this.needWait);

        return connection;
    }

    public boolean isNeedWait() {
        return needWait;
    }

    public void setNeedWait(boolean needWait) {
        this.needWait = needWait;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}

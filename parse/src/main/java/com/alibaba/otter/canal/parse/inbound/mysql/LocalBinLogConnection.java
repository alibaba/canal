package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.ServerIdNotMatchException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.local.BinLogFileQueue;
import com.taobao.tddl.dbsync.binlog.FileLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.LogPosition;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;

/**
 * local bin log connection (not real connection)
 * 
 * @author yuanzu Date: 12-9-27 Time: 下午6:14
 */
public class LocalBinLogConnection implements ErosaConnection {

    private static final Logger logger     = LoggerFactory.getLogger(LocalBinLogConnection.class);
    private BinLogFileQueue     binlogs    = null;
    private boolean             needWait;
    private String              directory;
    private int                 bufferSize = 16 * 1024;
    private boolean             running    = false;
    private long                serverId;
    private FileParserListener  parserListener;

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

    public void seek(String binlogfilename, Long binlogPosition, String gtid, SinkFunction func) throws IOException {
    }

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        File current = new File(directory, binlogfilename);

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current, binlogPosition);
            context.setLogPosition(new LogPosition(binlogfilename, binlogPosition));
            while (running) {
                boolean needContinue = true;
                LogEvent event = null;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        continue;
                    }
                    if (serverId != 0 && event.getServerId() != serverId) {
                        throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                    }

                    if (!func.sink(event)) {
                        needContinue = false;
                        break;
                    }
                }

                fetcher.close(); // 关闭上一个文件
                parserFinish(current.getName());
                if (needContinue) {// 读取下一个

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
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(nextFile.getName()));
                } else {
                    break;// 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinLogConnection dump interrupted");
        } finally {
            if (fetcher != null) {
                fetcher.close();
            }
        }
    }

    public void dump(long timestampMills, SinkFunction func) throws IOException {
        List<File> currentBinlogs = binlogs.currentBinlogs();
        File current = currentBinlogs.get(currentBinlogs.size() - 1);
        long timestampSeconds = timestampMills / 1000;

        String binlogFilename = null;
        long binlogFileOffset = 0;

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current);
            context.setLogPosition(new LogPosition(current.getName()));
            while (running) {
                boolean needContinue = true;
                String lastXidLogFilename = current.getName();
                long lastXidLogFileOffset = 0;

                binlogFilename = lastXidLogFilename;
                binlogFileOffset = lastXidLogFileOffset;
                while (fetcher.fetch()) {
                    LogEvent event = decoder.decode(fetcher, context);
                    if (event != null) {
                        if (serverId != 0 && event.getServerId() != serverId) {
                            throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                        }

                        if (event.getWhen() > timestampSeconds) {
                            break;
                        }

                        needContinue = false;
                        if (LogEvent.QUERY_EVENT == event.getHeader().getType()) {
                            if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN")) {
                                binlogFilename = lastXidLogFilename;
                                binlogFileOffset = lastXidLogFileOffset;
                            } else if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "COMMIT")) {
                                lastXidLogFilename = current.getName();
                                lastXidLogFileOffset = event.getLogPos();
                            }
                        } else if (LogEvent.XID_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        } else if (LogEvent.FORMAT_DESCRIPTION_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        }
                    }
                }

                if (needContinue) {// 读取下一个
                    fetcher.close(); // 关闭上一个文件

                    File nextFile = binlogs.getBefore(current);
                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(current.getName()));
                } else {
                    break;// 跳出
                }
            }
        } finally {
            if (fetcher != null) {
                fetcher.close();
            }
        }

        dump(binlogFilename, binlogFileOffset, func);
    }

    @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void dump(String binlogfilename, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException {
        File current = new File(directory, binlogfilename);
        if (!current.exists()) {
            throw new CanalParseException("binlog:" + binlogfilename + " is not found");
        }

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current, binlogPosition);
            context.setLogPosition(new LogPosition(binlogfilename, binlogPosition));
            while (running) {
                boolean needContinue = true;
                LogEvent event = null;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (event == null) {
                        continue;
                    }
                    if (serverId != 0 && event.getServerId() != serverId) {
                        throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                    }

                    if (!coprocessor.publish(event)) {
                        needContinue = false;
                        break;
                    }
                }

                fetcher.close(); // 关闭上一个文件
                parserFinish(binlogfilename);
                if (needContinue) {// 读取下一个
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
                    fetcher.open(current);
                    binlogfilename = nextFile.getName();
                } else {
                    break;// 跳出
                }
            }
        } catch (InterruptedException e) {
            logger.warn("LocalBinLogConnection dump interrupted");
        } finally {
            if (fetcher != null) {
                fetcher.close();
            }
        }
    }

    private void parserFinish(String fileName) {
        if (parserListener != null) {
            parserListener.onFinish(fileName);
        }
    }

    @Override
    public void dump(long timestampMills, MultiStageCoprocessor coprocessor) throws IOException {
        List<File> currentBinlogs = binlogs.currentBinlogs();
        File current = currentBinlogs.get(currentBinlogs.size() - 1);
        long timestampSeconds = timestampMills / 1000;

        String binlogFilename = null;
        long binlogFileOffset = 0;

        FileLogFetcher fetcher = new FileLogFetcher(bufferSize);
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        try {
            fetcher.open(current);
            context.setLogPosition(new LogPosition(current.getName()));
            while (running) {
                boolean needContinue = true;
                String lastXidLogFilename = current.getName();
                long lastXidLogFileOffset = 0;

                binlogFilename = lastXidLogFilename;
                binlogFileOffset = lastXidLogFileOffset;
                while (fetcher.fetch()) {
                    LogEvent event = decoder.decode(fetcher, context);
                    if (event != null) {
                        if (serverId != 0 && event.getServerId() != serverId) {
                            throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
                        }

                        if (event.getWhen() > timestampSeconds) {
                            break;
                        }

                        needContinue = false;
                        if (LogEvent.QUERY_EVENT == event.getHeader().getType()) {
                            if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "BEGIN")) {
                                binlogFilename = lastXidLogFilename;
                                binlogFileOffset = lastXidLogFileOffset;
                            } else if (StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(), "COMMIT")) {
                                lastXidLogFilename = current.getName();
                                lastXidLogFileOffset = event.getLogPos();
                            }
                        } else if (LogEvent.XID_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        } else if (LogEvent.FORMAT_DESCRIPTION_EVENT == event.getHeader().getType()) {
                            lastXidLogFilename = current.getName();
                            lastXidLogFileOffset = event.getLogPos();
                        }
                    }
                }

                if (needContinue) {// 读取下一个
                    fetcher.close(); // 关闭上一个文件

                    File nextFile = binlogs.getBefore(current);
                    if (nextFile == null) {
                        break;
                    }

                    current = nextFile;
                    fetcher.open(current);
                    context.setLogPosition(new LogPosition(current.getName()));
                } else {
                    break;// 跳出
                }
            }
        } finally {
            if (fetcher != null) {
                fetcher.close();
            }
        }

        dump(binlogFilename, binlogFileOffset, coprocessor);
    }

    @Override
    public void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NotImplementedException();
    }

    public ErosaConnection fork() {
        LocalBinLogConnection connection = new LocalBinLogConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setDirectory(this.directory);
        connection.setNeedWait(this.needWait);
        return connection;
    }

    @Override
    public long queryServerId() {
        return 0;
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

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public void setParserListener(FileParserListener parserListener) {
        this.parserListener = parserListener;
    }

    public interface FileParserListener {

        void onFinish(String fileName);
    }

}

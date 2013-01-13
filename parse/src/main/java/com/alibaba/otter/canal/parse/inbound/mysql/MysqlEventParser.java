package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.util.CollectionUtils;

import com.alibaba.erosa.parse.exceptions.TableIDParserException;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry.Entry;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * 基于向mysql server复制binlog实现
 * 
 * <pre>
 * 1. 自身不控制mysql主备切换，由ha机制来控制. 比如接入tddl/cobar/自身心跳包成功率
 * 2. 切换机制
 * </pre>
 * 
 * @author jianghang 2012-6-21 下午04:06:32
 * @version 1.0.0
 */
public class MysqlEventParser extends AbstractMysqlEventParser implements CanalEventParser, CanalHASwitchable {

    private HeartBeatCallback      heartBeatCallback                 = null;

    private int                    defaultConnectionTimeoutInSeconds = 30;       // sotimeout
    private int                    receiveBufferSize                 = 64 * 1024;
    private int                    sendBufferSize                    = 64 * 1024;
    // 数据库信息
    private AuthenticationInfo     masterInfo;                                   // 主库
    private AuthenticationInfo     standbyInfo;                                  // 备库
    // binlog信息
    private EntryPosition          masterPosition;
    private EntryPosition          standbyPosition;
    private long                   slaveId;                                      // 链接到mysql的slave
    // 心跳检查信息
    private volatile boolean       detectingEnable                   = true;     // 是否开启心跳检查
    private String                 detectingSQL;                                 // 心跳sql
    private Integer                detectingIntervalInSeconds        = 3;        // 检测频率
    private MysqlHeartBeatTimeTask mysqlHeartBeatTimeTask;
    // 心跳检查
    private volatile Timer         timer;

    @Override
    protected ErosaConnection buildErosaConnection() {
        return buildMysqlConnection(this.runningInfo);
    }

    @Override
    protected void preDump(ErosaConnection connection) {
        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        // 开始启动心跳包
        if (detectingEnable && StringUtils.isNotBlank(detectingSQL)) {
            logger.info("start heart beat.... ");
            startHeartbeat((MysqlConnection) connection);
        }
    }

    protected void afterDump(ErosaConnection connection) {
        super.afterDump(connection);

        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        // 开始启动心跳包
        if (detectingEnable && StringUtils.isNotBlank(detectingSQL)) {
            logger.info("stop heart beat.... ");
            stopHeartbeat();
        }
    }

    public void start() throws CanalParseException {
        if (runningInfo == null) { // 第一次链接主库
            runningInfo = masterInfo;
        }

        super.start();
    }

    public void stop() throws CanalParseException {
        stopHeartbeat();
        super.stop();
    }

    private void startHeartbeat(MysqlConnection mysqlConnection) {
        if (timer == null) {// lazy初始化一下
            synchronized (MysqlEventParser.class) {
                if (timer == null) {
                    timer = new Timer("MysqlHeartBeatTimeTask", true);
                }
            }
        }

        mysqlHeartBeatTimeTask = new MysqlHeartBeatTimeTask(mysqlConnection.fork());
        Integer interval = detectingIntervalInSeconds;
        timer.schedule(mysqlHeartBeatTimeTask, interval * 1000L, interval * 1000L);
    }

    private void stopHeartbeat() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        if (mysqlHeartBeatTimeTask != null) {
            MysqlConnection mysqlConnection = mysqlHeartBeatTimeTask.getMysqlConnection();
            try {
                mysqlConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect for address:{}", mysqlConnection.getAddress(), e);
            }

            mysqlHeartBeatTimeTask = null;
        }
    }

    /**
     * 心跳信息
     * 
     * @author jianghang 2012-7-6 下午02:50:15
     * @version 1.0.0
     */
    class MysqlHeartBeatTimeTask extends TimerTask {

        private boolean         reconnect = false;
        private MysqlConnection mysqlConnection;

        public MysqlHeartBeatTimeTask(MysqlConnection mysqlConnection){
            this.mysqlConnection = mysqlConnection;
        }

        public void run() {
            try {
                if (reconnect) {
                    reconnect = false;
                    mysqlConnection.reconnect();
                } else if (!mysqlConnection.getConnected().get()) {
                    mysqlConnection.connect();
                }

                try {
                    Long startTime = System.currentTimeMillis();
                    mysqlConnection.update(detectingSQL);
                    Long costTime = System.currentTimeMillis() - startTime;
                    if (heartBeatCallback != null) {
                        heartBeatCallback.onSuccess(costTime);
                    }
                } catch (SocketTimeoutException e) {
                    if (heartBeatCallback != null) {
                        heartBeatCallback.onFailed(e);
                    }
                    reconnect = true;
                } catch (IOException e) {
                    if (heartBeatCallback != null) {
                        heartBeatCallback.onFailed(e);
                    }
                    reconnect = true;
                }

            } catch (IOException e) {
                logger.warn("connect failed by " + ExceptionUtils.getStackTrace(e));
            }
        }

        public MysqlConnection getMysqlConnection() {
            return mysqlConnection;
        }
    }

    // 处理主备切换的逻辑
    public void doSwitch() {
        AuthenticationInfo newRunningInfo = (runningInfo.equals(masterInfo) ? standbyInfo : masterInfo);
        this.doSwitch(newRunningInfo);
    }

    @Override
    public void doSwitch(AuthenticationInfo newRunningInfo) {
        // 1. 需要停止当前正在复制的过程
        // 2. 找到新的position点
        // 3. 重新建立链接，开始复制数据
        // 切换ip
        String alarmMessage = null;

        if (this.runningInfo.equals(newRunningInfo)) {
            alarmMessage = "same runingInfo switch again : " + runningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            return;
        }

        if (newRunningInfo == null) {
            alarmMessage = "no standby config, just do nothing, will continue try:"
                           + runningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            sendAlarm(destination, alarmMessage);
            return;
        } else {
            stop();
            alarmMessage = "try to ha switch, old:" + runningInfo.getAddress().toString() + ", new:"
                           + newRunningInfo.getAddress().toString();
            logger.warn(alarmMessage);
            sendAlarm(destination, alarmMessage);
            runningInfo = newRunningInfo;
            start();
        }
    }

    // =================== helper method =================

    private MysqlConnection buildMysqlConnection(AuthenticationInfo runningInfo) {
        MysqlConnection connection = new MysqlConnection(this.slaveId);
        connection.setAddress(runningInfo.getAddress());

        connection.setUsername(runningInfo.getUsername());
        connection.setPassword(runningInfo.getPassword());
        connection.setDefaultSchema(runningInfo.getDefaultDatabaseName());

        connection.setCharset(connectionCharset);
        connection.setCharsetNumber(connectionCharsetNumber);
        connection.setReceiveBufferSize(receiveBufferSize);
        connection.setSendBufferSize(sendBufferSize);
        connection.setSoTimeout(defaultConnectionTimeoutInSeconds * 1000);
        return connection;
    }

    protected EntryPosition findStartPosition(ErosaConnection connection) throws IOException {
        EntryPosition startPosition = findStartPositionInternal(connection);

        logger.info("prepare to find last position : {}", startPosition.toString());
        Long preTransactionStartPosition = findTransactionBeginPosition(connection, startPosition);
        if (!preTransactionStartPosition.equals(startPosition.getPosition())) {
            logger.info("find new start Transaction Position , old : {} , new : {}", startPosition.getPosition(),
                        preTransactionStartPosition);
            startPosition.setPosition(preTransactionStartPosition);
        }

        return startPosition;
    }

    protected EntryPosition findStartPositionInternal(ErosaConnection connection) {
        MysqlConnection mysqlConnection = (MysqlConnection) connection;
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {// 找不到历史成功记录
            EntryPosition entryPosition = null;
            if (masterInfo != null && mysqlConnection.getAddress().equals(masterInfo.getAddress())) {
                entryPosition = masterPosition;
            } else if (standbyInfo != null && mysqlConnection.getAddress().equals(standbyInfo.getAddress())) {
                entryPosition = standbyPosition;
            }

            if (entryPosition == null) {
                entryPosition = findEndPosition(mysqlConnection); // 默认从当前最后一个位置进行消费
            }

            // 判断一下是否需要按时间订阅
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // 如果没有指定binlogName，尝试按照timestamp进行查找
                if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                    return findByStartTimeStamp(mysqlConnection, entryPosition.getTimestamp());
                } else {
                    return null;
                }
            } else {
                if (entryPosition.getPosition() != null && entryPosition.getPosition() > 0L) {
                    // 如果指定binlogName + offest，直接返回
                    return entryPosition;
                } else {
                    EntryPosition specificLogFilePosition = null;
                    if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                        // 如果指定binlogName + timestamp，但没有指定对应的offest，尝试根据时间找一下offest
                        EntryPosition endPosition = findEndPosition(mysqlConnection);
                        if (endPosition != null) {
                            specificLogFilePosition = findAsPerTimestampInSpecificLogFile(
                                                                                          mysqlConnection,
                                                                                          entryPosition.getTimestamp(),
                                                                                          endPosition,
                                                                                          entryPosition.getJournalName());
                        }
                    }

                    if (specificLogFilePosition == null) {
                        // position不存在，从文件头开始
                        entryPosition.setPosition(BINLOG_START_OFFEST);
                        return entryPosition;
                    } else {
                        return specificLogFilePosition;
                    }
                }
            }
        } else {
            if (logPosition.getIdentity().getSourceAddress().equals(mysqlConnection.getAddress())) {
                return logPosition.getPostion();
            } else {
                return findByStartTimeStamp(mysqlConnection, logPosition.getPostion().getTimestamp());
            }
        }
    }

    // 根据想要的position，可能这个position对应的记录为rowdata，需要找到事务头，避免丢数据
    // 主要考虑一个事务执行时间可能会几秒种，如果仅仅按照timestamp相同，则可能会丢失事务的前半部分数据
    private Long findTransactionBeginPosition(ErosaConnection mysqlConnection, final EntryPosition entryPosition)
                                                                                                                 throws IOException {
        // 尝试找到一个合适的位置
        final AtomicBoolean reDump = new AtomicBoolean(false);
        mysqlConnection.reconnect();
        mysqlConnection.dump(entryPosition.getJournalName(), entryPosition.getPosition(), new SinkFunction() {

            private LogPosition lastPosition;

            public boolean sink(byte[] data) {
                try {
                    List<ErosaEntry.Entry> entries = parseAndProfilingIfNecessary(data);
                    if (!CollectionUtils.isEmpty(entries)) {
                        for (ErosaEntry.Entry entry : entries) {
                            // 直接查询第一条业务数据，确认是否为事务Begin/End
                            if (ErosaEntry.EntryType.MYSQL_FORMATDESCRIPTION == entry.getEntryType()
                                || ErosaEntry.EntryType.MYSQL_ROTATE == entry.getEntryType()) {
                                // do nothing
                                continue;
                            } else if (ErosaEntry.EntryType.TRANSACTIONBEGIN == entry.getEntryType()
                                       || ErosaEntry.EntryType.TRANSACTIONEND == entry.getEntryType()) {
                                lastPosition = buildLastPosition(entries);
                                return false;
                            } else {
                                reDump.set(true);
                                lastPosition = buildLastPosition(entries);
                                return false;
                            }

                        }
                    }

                } catch (TableIDParserException e) {
                    // 上一次记录的poistion可能为一条update/insert/delete变更事件，直接进行dump的话，会缺少tableMap事件，导致tableId未进行解析
                    processError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                    reDump.set(true);
                    return false;
                } catch (Exception e) {
                    processError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                    reDump.set(true);
                    return false;
                }

                return running;
            }
        });
        // 针对开始的第一条为非Begin记录，需要从该binlog扫描
        if (reDump.get()) {
            final AtomicLong preTransactionStartPosition = new AtomicLong(0L);
            mysqlConnection.reconnect();
            mysqlConnection.dump(entryPosition.getJournalName(), 4L, new SinkFunction() {

                private LogPosition lastPosition;

                public boolean sink(byte[] data) {
                    try {
                        List<ErosaEntry.Entry> entrys = parseAndProfilingIfNecessary(data);
                        if (!CollectionUtils.isEmpty(entrys)) {
                            for (ErosaEntry.Entry entry : entrys) {
                                // 直接查询第一条业务数据，确认是否为事务Begin
                                if (!(ErosaEntry.EntryType.MYSQL_FORMATDESCRIPTION == entry.getEntryType()
                                      || ErosaEntry.EntryType.MYSQL_ROTATE == entry.getEntryType() || ErosaEntry.EntryType.MYSQL_STOP == entry.getEntryType())) {
                                    // 记录一下transaction begin position
                                    if (entry.getEntryType() == ErosaEntry.EntryType.TRANSACTIONBEGIN
                                        && entry.getHeader().getLogfileoffset() < entryPosition.getPosition()) {
                                        preTransactionStartPosition.set(entry.getHeader().getLogfileoffset());
                                    }

                                    if (entry.getHeader().getLogfileoffset() >= entryPosition.getPosition()) {
                                        return false;// 退出
                                    }

                                    lastPosition = buildLastPosition(entrys);
                                }
                            }

                        }

                    } catch (Exception e) {
                        processError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                        return false;
                    }

                    return running;
                }
            });

            // 判断一下找到的最接近position的事务头的位置
            if (preTransactionStartPosition.get() > entryPosition.getPosition()) {
                logger.error("preTransactionEndPosition greater than startPosition from zk or localconf, maybe lost data");
                throw new CanalParseException(
                                              "preTransactionStartPosition greater than startPosition from zk or localconf, maybe lost data");
            }
            return preTransactionStartPosition.get();
        } else {
            return entryPosition.getPosition();
        }
    }

    // 根据时间查找binlog位置
    private EntryPosition findByStartTimeStamp(MysqlConnection mysqlConnection, Long startTimestamp) {
        EntryPosition endPosition = findEndPosition(mysqlConnection);
        logger.info("show master status to set search end condition:{} ", endPosition);
        String startSearchBinlogFile = endPosition.getJournalName();
        boolean shouldBreak = false;
        while (running && !shouldBreak) {
            try {
                EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(mysqlConnection, startTimestamp,
                                                                                  endPosition, startSearchBinlogFile);
                if (entryPosition == null) {
                    // 继续往前找
                    int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                    if (binlogSeqNum <= 1) {
                        logger.warn("Didn't find the corresponding binlog files");
                        shouldBreak = true;
                    } else {
                        int nextBinlogSeqNum = binlogSeqNum - 1;
                        String binlogFileNamePrefix = startSearchBinlogFile.substring(
                                                                                      0,
                                                                                      startSearchBinlogFile.indexOf(".") + 1);
                        String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                        startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                    }
                } else {
                    logger.info("found and return:{} in findByStartTimeStamp operation.", entryPosition);
                    return entryPosition;
                }
            } catch (Exception e) {
                logger.info("the binlogfile:{} doesn't exist, to continue to search the next binlogfile");
                int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                if (binlogSeqNum <= 1) {
                    logger.warn("Didn't find the corresponding binlog files");
                    shouldBreak = true;
                } else {
                    int nextBinlogSeqNum = binlogSeqNum - 1;
                    String binlogFileNamePrefix = startSearchBinlogFile.substring(
                                                                                  0,
                                                                                  startSearchBinlogFile.indexOf(".") + 1);
                    String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                    startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                }
            }
        }
        // 找不到
        return null;
    }

    /**
     * 查询当前的binlog位置
     */
    private EntryPosition findEndPosition(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show master status");
            List<String> fields = packet.getFieldValues();
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            return endPosition;
        } catch (IOException e) {
            logger.error("find end position error", e);
        }

        return null;
    }

    /**
     * 根据给定的时间戳，在指定的binlog中找到最接近于该时间戳(必须是小于时间戳)的一个事务起始位置。针对最后一个binlog会给定endPosition，避免无尽的查询
     */
    private EntryPosition findAsPerTimestampInSpecificLogFile(MysqlConnection mysqlConnection,
                                                              final Long startTimestamp,
                                                              final EntryPosition endPosition,
                                                              final String searchBinlogFile) {

        final LogPosition logPosition = new LogPosition();
        try {
            mysqlConnection.reconnect();
            // 开始遍历文件
            mysqlConnection.dump(searchBinlogFile, 4L, new SinkFunction() {

                private LogPosition lastPosition;

                public boolean sink(byte[] data) {
                    EntryPosition entryPosition = null;
                    try {
                        List<Entry> entrys = parseAndProfilingIfNecessary(data);
                        if (!CollectionUtils.isEmpty(entrys)) {
                            for (Entry entry : entrys) {
                                String logfilename = entry.getHeader().getLogfilename();
                                Long logfileoffset = entry.getHeader().getLogfileoffset();
                                Long logposTimestamp = entry.getHeader().getExecutetime();

                                if (ErosaEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType())) {
                                    logger.debug("compare exit condition:{},{},{}, startTimestamp={}...", new Object[] {
                                            logfilename, logfileoffset, logposTimestamp, startTimestamp });
                                    // 寻找第一条记录时间戳，如果最小的一条记录都不满足条件，可直接退出
                                    if (logposTimestamp >= startTimestamp) {
                                        return false;
                                    }
                                }

                                if (StringUtils.equals(endPosition.getJournalName(), logfilename)
                                    && endPosition.getPosition() <= (logfileoffset + data.length)) {
                                    return false;
                                }

                                // 记录一下上一个事务结束的位置，即下一个事务的position
                                // position = current + data.length，代表该事务的下一条offest，避免多余的事务重复
                                if (ErosaEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())) {
                                    entryPosition = new EntryPosition(logfilename, logfileoffset + data.length,
                                                                      logposTimestamp);
                                    logger.debug(
                                                 "set {} to be pending start position before finding another proper one...",
                                                 entryPosition);
                                    logPosition.setPostion(entryPosition);
                                }

                                lastPosition = buildLastPosition(entrys);
                            }
                        } else {
                            logger.info("no binary events is parsed out.");
                        }
                    } catch (Exception e) {
                        processError(e, lastPosition, searchBinlogFile, 4L);
                    }

                    return running;
                }
            });

        } catch (IOException e) {
            logger.error("ERROR ## findAsPerTimestampInSpecificLogFile has an error", e);
        }

        if (logPosition.getPostion() != null) {
            return logPosition.getPostion();
        } else {
            return null;
        }
    }

    // ===================== setter / getter ========================

    public void setHeartBeatCallback(HeartBeatCallback heartBeatCallback) {
        this.heartBeatCallback = heartBeatCallback;
    }

    public void setDefaultConnectionTimeoutInSeconds(int defaultConnectionTimeoutInSeconds) {
        this.defaultConnectionTimeoutInSeconds = defaultConnectionTimeoutInSeconds;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public void setMasterInfo(AuthenticationInfo masterInfo) {
        this.masterInfo = masterInfo;
    }

    public void setStandbyInfo(AuthenticationInfo standbyInfo) {
        this.standbyInfo = standbyInfo;
    }

    public void setMasterPosition(EntryPosition masterPosition) {
        this.masterPosition = masterPosition;
    }

    public void setStandbyPosition(EntryPosition standbyPosition) {
        this.standbyPosition = standbyPosition;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public void setDetectingSQL(String detectingSQL) {
        this.detectingSQL = detectingSQL;
    }

    public void setDetectingIntervalInSeconds(Integer detectingIntervalInSeconds) {
        this.detectingIntervalInSeconds = detectingIntervalInSeconds;
    }

    public void setDetectingEnable(boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

}

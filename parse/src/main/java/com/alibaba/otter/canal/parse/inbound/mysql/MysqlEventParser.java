package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.BinlogFormat;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.BinlogImage;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.taobao.tddl.dbsync.binlog.LogEvent;

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

    private CanalHAController    haController                      = null;

    private int                  defaultConnectionTimeoutInSeconds = 30;       // sotimeout
    private int                  receiveBufferSize                 = 64 * 1024;
    private int                  sendBufferSize                    = 64 * 1024;
    // 数据库信息
    protected AuthenticationInfo masterInfo;                                   // 主库
    protected AuthenticationInfo standbyInfo;                                  // 备库
    // binlog信息
    protected EntryPosition      masterPosition;
    protected EntryPosition      standbyPosition;
    private long                 slaveId;                                      // 链接到mysql的slave
    // 心跳检查信息
    private String               detectingSQL;                                 // 心跳sql
    private MysqlConnection      metaConnection;                               // 查询meta信息的链接
    private TableMetaCache       tableMetaCache;                               // 对应meta
    private int                  fallbackIntervalInSeconds         = 60;       // 切换回退时间
    private BinlogFormat[]       supportBinlogFormats;                         // 支持的binlogFormat,如果设置会执行强校验
    private BinlogImage[]        supportBinlogImages;                          // 支持的binlogImage,如果设置会执行强校验

    // update by yishun.chen,特殊异常处理参数
    private int                  dumpErrorCount                    = 0;        // binlogDump失败异常计数
    private int                  dumpErrorCountThreshold           = 2;        // binlogDump失败异常计数阀值
    private boolean              rdsOssMode                        = false;
    private boolean              autoResetLatestPosMode            = false;    // true: binlog被删除之后，自动按最新的数据订阅

    protected ErosaConnection buildErosaConnection() {
        return buildMysqlConnection(this.runningInfo);
    }

    protected void preDump(ErosaConnection connection) {
        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        if (binlogParser != null && binlogParser instanceof LogEventConvert) {
            metaConnection = (MysqlConnection) connection.fork();
            try {
                metaConnection.connect();
            } catch (IOException e) {
                throw new CanalParseException(e);
            }

            if (supportBinlogFormats != null && supportBinlogFormats.length > 0) {
                BinlogFormat format = ((MysqlConnection) metaConnection).getBinlogFormat();
                boolean found = false;
                for (BinlogFormat supportFormat : supportBinlogFormats) {
                    if (supportFormat != null && format == supportFormat) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    throw new CanalParseException("Unsupported BinlogFormat " + format);
                }
            }

            if (supportBinlogImages != null && supportBinlogImages.length > 0) {
                BinlogImage image = ((MysqlConnection) metaConnection).getBinlogImage();
                boolean found = false;
                for (BinlogImage supportImage : supportBinlogImages) {
                    if (supportImage != null && image == supportImage) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    throw new CanalParseException("Unsupported BinlogImage " + image);
                }
            }

            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setConnection(metaConnection);
                ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
                ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
                ((DatabaseTableMeta) tableMetaTSDB).setSnapshotInterval(tsdbSnapshotInterval);
                ((DatabaseTableMeta) tableMetaTSDB).setSnapshotExpire(tsdbSnapshotExpire);
                ((DatabaseTableMeta) tableMetaTSDB).init(destination);
            }

            tableMetaCache = new TableMetaCache(metaConnection, tableMetaTSDB);
            ((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
        }
    }

    protected void afterDump(ErosaConnection connection) {
        super.afterDump(connection);

        if (connection == null) {
            throw new CanalParseException("illegal connection is null");
        }

        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector()
                    .getAddress(), e);
            }
        }
    }

    public void start() throws CanalParseException {
        if (runningInfo == null) { // 第一次链接主库
            runningInfo = masterInfo;
        }

        super.start();
    }

    public void stop() throws CanalParseException {
        if (metaConnection != null) {
            try {
                metaConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect meta connection for address:{}", metaConnection.getConnector()
                    .getAddress(), e);
            }
        }

        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }

        super.stop();
    }

    protected TimerTask buildHeartBeatTimeTask(ErosaConnection connection) {
        if (!(connection instanceof MysqlConnection)) {
            throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
        }

        // 开始mysql心跳sql
        if (detectingEnable && StringUtils.isNotBlank(detectingSQL)) {
            return new MysqlDetectingTimeTask((MysqlConnection) connection.fork());
        } else {
            return super.buildHeartBeatTimeTask(connection);
        }

    }

    protected void stopHeartBeat() {
        TimerTask heartBeatTimerTask = this.heartBeatTimerTask;
        super.stopHeartBeat();
        if (heartBeatTimerTask != null && heartBeatTimerTask instanceof MysqlDetectingTimeTask) {
            MysqlConnection mysqlConnection = ((MysqlDetectingTimeTask) heartBeatTimerTask).getMysqlConnection();
            try {
                mysqlConnection.disconnect();
            } catch (IOException e) {
                logger.error("ERROR # disconnect heartbeat connection for address:{}", mysqlConnection.getConnector()
                    .getAddress(), e);
            }
        }
    }

    /**
     * 心跳信息
     *
     * @author jianghang 2012-7-6 下午02:50:15
     * @version 1.0.0
     */
    class MysqlDetectingTimeTask extends TimerTask {

        private boolean         reconnect = false;
        private MysqlConnection mysqlConnection;

        public MysqlDetectingTimeTask(MysqlConnection mysqlConnection){
            this.mysqlConnection = mysqlConnection;
        }

        public void run() {
            try {
                if (reconnect) {
                    reconnect = false;
                    mysqlConnection.reconnect();
                } else if (!mysqlConnection.isConnected()) {
                    mysqlConnection.connect();
                }
                long startTime = System.currentTimeMillis();

                // 可能心跳sql为select 1
                if (StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "select")
                    || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "show")
                    || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "explain")
                    || StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "desc")) {
                    mysqlConnection.query(detectingSQL);
                } else {
                    mysqlConnection.update(detectingSQL);
                }

                long costTime = System.currentTimeMillis() - startTime;
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onSuccess(costTime);
                }
            } catch (Throwable e) {
                if (haController != null && haController instanceof HeartBeatCallback) {
                    ((HeartBeatCallback) haController).onFailed(e);
                }
                reconnect = true;
                logger.warn("connect failed by ", e);
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
        MysqlConnection connection = new MysqlConnection(runningInfo.getAddress(),
            runningInfo.getUsername(),
            runningInfo.getPassword(),
            connectionCharsetNumber,
            runningInfo.getDefaultDatabaseName());
        connection.getConnector().setReceiveBufferSize(receiveBufferSize);
        connection.getConnector().setSendBufferSize(sendBufferSize);
        connection.getConnector().setSoTimeout(defaultConnectionTimeoutInSeconds * 1000);
        connection.setCharset(connectionCharset);
        connection.setReceivedBinlogBytes(receivedBinlogBytes);
        // 随机生成slaveId
        if (this.slaveId <= 0) {
            this.slaveId = generateUniqueServerId();
        }
        connection.setSlaveId(this.slaveId);
        return connection;
    }

    private final long generateUniqueServerId() {
        try {
            // a=`echo $masterip|cut -d\. -f1`
            // b=`echo $masterip|cut -d\. -f2`
            // c=`echo $masterip|cut -d\. -f3`
            // d=`echo $masterip|cut -d\. -f4`
            // #server_id=`expr $a \* 256 \* 256 \* 256 + $b \* 256 \* 256 + $c
            // \* 256 + $d `
            // #server_id=$b$c$d
            // server_id=`expr $b \* 256 \* 256 + $c \* 256 + $d `
            InetAddress localHost = InetAddress.getLocalHost();
            byte[] addr = localHost.getAddress();
            int salt = (destination != null) ? destination.hashCode() : 0;
            return ((0x7f & salt) << 24) + ((0xff & (int) addr[1]) << 16) // NL
                   + ((0xff & (int) addr[2]) << 8) // NL
                   + (0xff & (int) addr[3]);
        } catch (UnknownHostException e) {
            throw new CanalParseException("Unknown host", e);
        }
    }

    protected EntryPosition findStartPosition(ErosaConnection connection) throws IOException {
        if (isGTIDMode()) {
            // GTID模式下，CanalLogPositionManager里取最后的gtid，没有则取instanc配置中的
            LogPosition logPosition = getLogPositionManager().getLatestIndexBy(destination);
            if (logPosition != null) {
                // 如果以前是非GTID模式，后来调整为了GTID模式，那么为了保持兼容，需要判断gtid是否为空
                if (StringUtils.isNotEmpty(logPosition.getPostion().getGtid())) {
                    return logPosition.getPostion();
                }
            }else {
                if (masterPosition != null && StringUtils.isNotEmpty(masterPosition.getGtid())) {
                    return masterPosition;
                }
            }
        }

        EntryPosition startPosition = findStartPositionInternal(connection);
        if (needTransactionPosition.get()) {
            logger.warn("prepare to find last position : {}", startPosition.toString());
            Long preTransactionStartPosition = findTransactionBeginPosition(connection, startPosition);
            if (!preTransactionStartPosition.equals(startPosition.getPosition())) {
                logger.warn("find new start Transaction Position , old : {} , new : {}",
                    startPosition.getPosition(),
                    preTransactionStartPosition);
                startPosition.setPosition(preTransactionStartPosition);
            }
            needTransactionPosition.compareAndSet(true, false);
        }
        return startPosition;
    }

    protected EntryPosition findEndPosition(ErosaConnection connection) throws IOException {
        MysqlConnection mysqlConnection = (MysqlConnection) connection;
        EntryPosition endPosition = findEndPosition(mysqlConnection);
        return endPosition;
    }

    protected EntryPosition findEndPositionWithMasterIdAndTimestamp(MysqlConnection connection) {
        MysqlConnection mysqlConnection = (MysqlConnection) connection;
        final EntryPosition endPosition = findEndPosition(mysqlConnection);
        if (tableMetaTSDB != null) {
            long startTimestamp = System.currentTimeMillis();
            return findAsPerTimestampInSpecificLogFile(mysqlConnection,
                startTimestamp,
                endPosition,
                endPosition.getJournalName(),
                true);
        } else {
            return endPosition;
        }
    }

    protected EntryPosition findPositionWithMasterIdAndTimestamp(MysqlConnection connection, EntryPosition fixedPosition) {
        MysqlConnection mysqlConnection = (MysqlConnection) connection;
        if (tableMetaTSDB != null && (fixedPosition.getTimestamp() == null || fixedPosition.getTimestamp() <= 0)) {
            // 使用一个未来极大的时间，基于位点进行定位
            long startTimestamp = System.currentTimeMillis() + 102L * 365 * 24 * 3600 * 1000; // 当前时间的未来102年
            EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(mysqlConnection,
                startTimestamp,
                fixedPosition,
                fixedPosition.getJournalName(),
                true);
            if (entryPosition == null) {
                throw new CanalParseException("[fixed timestamp] can't found begin/commit position before with fixed position"
                                              + fixedPosition.getJournalName() + ":" + fixedPosition.getPosition());
            }
            return entryPosition;
        } else {
            return fixedPosition;
        }
    }

    protected EntryPosition findStartPositionInternal(ErosaConnection connection) {
        MysqlConnection mysqlConnection = (MysqlConnection) connection;
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {// 找不到历史成功记录
            EntryPosition entryPosition = null;
            if (masterInfo != null && mysqlConnection.getConnector().getAddress().equals(masterInfo.getAddress())) {
                entryPosition = masterPosition;
            } else if (standbyInfo != null
                       && mysqlConnection.getConnector().getAddress().equals(standbyInfo.getAddress())) {
                entryPosition = standbyPosition;
            }

            if (entryPosition == null) {
                entryPosition = findEndPositionWithMasterIdAndTimestamp(mysqlConnection); // 默认从当前最后一个位置进行消费
            }

            // 判断一下是否需要按时间订阅
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // 如果没有指定binlogName，尝试按照timestamp进行查找
                if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                    logger.warn("prepare to find start position {}:{}:{}",
                        new Object[] { "", "", entryPosition.getTimestamp() });
                    return findByStartTimeStamp(mysqlConnection, entryPosition.getTimestamp());
                } else {
                    logger.warn("prepare to find start position just show master status");
                    return findEndPositionWithMasterIdAndTimestamp(mysqlConnection); // 默认从当前最后一个位置进行消费
                }
            } else {
                if (entryPosition.getPosition() != null && entryPosition.getPosition() > 0L) {
                    // 如果指定binlogName + offest，直接返回
                    entryPosition = findPositionWithMasterIdAndTimestamp(mysqlConnection, entryPosition);
                    logger.warn("prepare to find start position {}:{}:{}",
                        new Object[] { entryPosition.getJournalName(), entryPosition.getPosition(),
                                entryPosition.getTimestamp() });
                    return entryPosition;
                } else {
                    EntryPosition specificLogFilePosition = null;
                    if (entryPosition.getTimestamp() != null && entryPosition.getTimestamp() > 0L) {
                        // 如果指定binlogName +
                        // timestamp，但没有指定对应的offest，尝试根据时间找一下offest
                        EntryPosition endPosition = findEndPosition(mysqlConnection);
                        if (endPosition != null) {
                            logger.warn("prepare to find start position {}:{}:{}",
                                new Object[] { entryPosition.getJournalName(), "", entryPosition.getTimestamp() });
                            specificLogFilePosition = findAsPerTimestampInSpecificLogFile(mysqlConnection,
                                entryPosition.getTimestamp(),
                                endPosition,
                                entryPosition.getJournalName(),
                                true);
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
            if (logPosition.getIdentity().getSourceAddress().equals(mysqlConnection.getConnector().getAddress())) {
                if (dumpErrorCountThreshold >= 0 && dumpErrorCount > dumpErrorCountThreshold) {
                    // binlog定位位点失败,可能有两个原因:
                    // 1. binlog位点被删除
                    // 2.vip模式的mysql,发生了主备切换,判断一下serverId是否变化,针对这种模式可以发起一次基于时间戳查找合适的binlog位点
                    boolean case2 = (standbyInfo == null || standbyInfo.getAddress() == null)
                                    && logPosition.getPostion().getServerId() != null
                                    && !logPosition.getPostion().getServerId().equals(findServerId(mysqlConnection));
                    if (case2) {
                        EntryPosition findPosition = fallbackFindByStartTimestamp(logPosition, mysqlConnection);
                        dumpErrorCount = 0;
                        return findPosition;
                    }
                    // 处理 binlog 位点被删除的情况，提供自动重置到当前位点的功能
                    // 应用场景: 测试环境不稳定，位点经常被删。强烈不建议在正式环境中开启此控制参数，因为binlog 丢失调到最新位点也即意味着数据丢失
                    if (isAutoResetLatestPosMode()) {
                        dumpErrorCount = 0;
                        return findEndPosition(mysqlConnection);
                    }
                    Long timestamp = logPosition.getPostion().getTimestamp();
                    if (isRdsOssMode() && (timestamp != null && timestamp > 0)) {
                        // 如果binlog位点不存在，并且属于timestamp不为空,可以返回null走到oss binlog处理
                        return null;
                    }
                } else if (StringUtils.isBlank(logPosition.getPostion().getJournalName())
                        && logPosition.getPostion().getPosition() <= 0
                        && logPosition.getPostion().getTimestamp() > 0) {
                    return fallbackFindByStartTimestamp(logPosition,mysqlConnection);
                }
                // 其余情况
                logger.warn("prepare to find start position just last position\n {}",
                    JsonUtils.marshalToString(logPosition));
                return logPosition.getPostion();
            } else {
                // 针对切换的情况，考虑回退时间
                long newStartTimestamp = logPosition.getPostion().getTimestamp() - fallbackIntervalInSeconds * 1000;
                logger.warn("prepare to find start position by switch {}:{}:{}", new Object[] { "", "",
                        logPosition.getPostion().getTimestamp() });
                return findByStartTimeStamp(mysqlConnection, newStartTimestamp);
            }
        }
    }

    /**
     * find position by timestamp with a fallback interval seconds.
     *
     * @param logPosition
     * @param mysqlConnection
     * @return
     */
    protected EntryPosition fallbackFindByStartTimestamp(LogPosition logPosition,MysqlConnection mysqlConnection){
        long timestamp = logPosition.getPostion().getTimestamp();
        long newStartTimestamp = timestamp - fallbackIntervalInSeconds * 1000;
        logger.warn("prepare to find start position by last position {}:{}:{}", new Object[] { "", "",
                logPosition.getPostion().getTimestamp() });
        return findByStartTimeStamp(mysqlConnection, newStartTimestamp);
    }

    // 根据想要的position，可能这个position对应的记录为rowdata，需要找到事务头，避免丢数据
    // 主要考虑一个事务执行时间可能会几秒种，如果仅仅按照timestamp相同，则可能会丢失事务的前半部分数据
    private Long findTransactionBeginPosition(ErosaConnection mysqlConnection, final EntryPosition entryPosition)
                                                                                                                 throws IOException {
        // 针对开始的第一条为非Begin记录，需要从该binlog扫描
        final java.util.concurrent.atomic.AtomicLong preTransactionStartPosition = new java.util.concurrent.atomic.AtomicLong(0L);
        mysqlConnection.reconnect();
        mysqlConnection.seek(entryPosition.getJournalName(), 4L, entryPosition.getGtid(), new SinkFunction<LogEvent>() {

            private LogPosition lastPosition;

            public boolean sink(LogEvent event) {
                try {
                    CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, true);
                    if (entry == null) {
                        return true;
                    }

                    // 直接查询第一条业务数据，确认是否为事务Begin
                    // 记录一下transaction begin position
                    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                        && entry.getHeader().getLogfileOffset() < entryPosition.getPosition()) {
                        preTransactionStartPosition.set(entry.getHeader().getLogfileOffset());
                    }

                    if (entry.getHeader().getLogfileOffset() >= entryPosition.getPosition()) {
                        return false;// 退出
                    }

                    lastPosition = buildLastPosition(entry);
                } catch (Exception e) {
                    processSinkError(e, lastPosition, entryPosition.getJournalName(), entryPosition.getPosition());
                    return false;
                }

                return running;
            }
        });

        // 判断一下找到的最接近position的事务头的位置
        if (preTransactionStartPosition.get() > entryPosition.getPosition()) {
            logger.error("preTransactionEndPosition greater than startPosition from zk or localconf, maybe lost data");
            throw new CanalParseException("preTransactionStartPosition greater than startPosition from zk or localconf, maybe lost data");
        }
        return preTransactionStartPosition.get();
    }

    // 根据时间查找binlog位置
    private EntryPosition findByStartTimeStamp(MysqlConnection mysqlConnection, Long startTimestamp) {
        EntryPosition endPosition = findEndPosition(mysqlConnection);
        EntryPosition startPosition = findStartPosition(mysqlConnection);
        String maxBinlogFileName = endPosition.getJournalName();
        String minBinlogFileName = startPosition.getJournalName();
        logger.info("show master status to set search end condition:{} ", endPosition);
        String startSearchBinlogFile = endPosition.getJournalName();
        boolean shouldBreak = false;
        while (running && !shouldBreak) {
            try {
                EntryPosition entryPosition = findAsPerTimestampInSpecificLogFile(mysqlConnection,
                    startTimestamp,
                    endPosition,
                    startSearchBinlogFile,
                    false);
                if (entryPosition == null) {
                    if (StringUtils.equalsIgnoreCase(minBinlogFileName, startSearchBinlogFile)) {
                        // 已经找到最早的一个binlog，没必要往前找了
                        shouldBreak = true;
                        logger.warn("Didn't find the corresponding binlog files from {} to {}",
                            minBinlogFileName,
                            maxBinlogFileName);
                    } else {
                        // 继续往前找
                        int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                        if (binlogSeqNum <= 1) {
                            logger.warn("Didn't find the corresponding binlog files");
                            shouldBreak = true;
                        } else {
                            int nextBinlogSeqNum = binlogSeqNum - 1;
                            String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
                                startSearchBinlogFile.indexOf(".") + 1);
                            String binlogFileNameSuffix = String.format("%06d", nextBinlogSeqNum);
                            startSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix;
                        }
                    }
                } else {
                    logger.info("found and return:{} in findByStartTimeStamp operation.", entryPosition);
                    return entryPosition;
                }
            } catch (Exception e) {
                logger.warn(String.format("the binlogfile:%s doesn't exist, to continue to search the next binlogfile , caused by",
                    startSearchBinlogFile),
                    e);
                int binlogSeqNum = Integer.parseInt(startSearchBinlogFile.substring(startSearchBinlogFile.indexOf(".") + 1));
                if (binlogSeqNum <= 1) {
                    logger.warn("Didn't find the corresponding binlog files");
                    shouldBreak = true;
                } else {
                    int nextBinlogSeqNum = binlogSeqNum - 1;
                    String binlogFileNamePrefix = startSearchBinlogFile.substring(0,
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
     * 查询当前db的serverId信息
     */
    private Long findServerId(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show variables like 'server_id'");
            List<String> fields = packet.getFieldValues();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CanalParseException("command : show variables like 'server_id' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            return Long.valueOf(fields.get(1));
        } catch (IOException e) {
            throw new CanalParseException("command : show variables like 'server_id' has an error!", e);
        }
    }

    /**
     * 查询当前的binlog位置
     */
    private EntryPosition findEndPosition(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show master status");
            List<String> fields = packet.getFieldValues();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CanalParseException("command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            if (isGTIDMode() && fields.size() > 4) {
                endPosition.setGtid(fields.get(4));
            }
            // MariaDB 无法通过`show master status`获取 gtid
            if (mysqlConnection.isMariaDB() && isGTIDMode()) {
                ResultSetPacket gtidPacket = mysqlConnection.query("SELECT @@global.gtid_binlog_pos");
                List<String> gtidFields = gtidPacket.getFieldValues();
                if (!CollectionUtils.isEmpty(gtidFields) && gtidFields.size() > 0) {
                    endPosition.setGtid(gtidFields.get(0));
                }
            }
            return endPosition;
        } catch (IOException e) {
            throw new CanalParseException("command : 'show master status' has an error!", e);
        }
    }

    /**
     * 查询当前的binlog位置
     */
    private EntryPosition findStartPosition(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show binlog events limit 1");
            List<String> fields = packet.getFieldValues();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CanalParseException("command : 'show binlog events limit 1' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0), Long.valueOf(fields.get(1)));
            return endPosition;
        } catch (IOException e) {
            throw new CanalParseException("command : 'show binlog events limit 1' has an error!", e);
        }

    }

    /**
     * 查询当前的slave视图的binlog位置
     */
    @SuppressWarnings("unused")
    private SlaveEntryPosition findSlavePosition(MysqlConnection mysqlConnection) {
        try {
            ResultSetPacket packet = mysqlConnection.query("show slave status");
            List<FieldPacket> names = packet.getFieldDescriptors();
            List<String> fields = packet.getFieldValues();
            if (CollectionUtils.isEmpty(fields)) {
                return null;
            }

            int i = 0;
            Map<String, String> maps = new HashMap<>(names.size(), 1f);
            for (FieldPacket name : names) {
                maps.put(name.getName(), fields.get(i));
                i++;
            }

            String errno = maps.get("Last_Errno");
            String slaveIORunning = maps.get("Slave_IO_Running"); // Slave_SQL_Running
            String slaveSQLRunning = maps.get("Slave_SQL_Running"); // Slave_SQL_Running
            if ((!"0".equals(errno)) || (!"Yes".equalsIgnoreCase(slaveIORunning))
                || (!"Yes".equalsIgnoreCase(slaveSQLRunning))) {
                logger.warn("Ignoring failed slave: " + mysqlConnection.getConnector().getAddress() + ", Last_Errno = "
                            + errno + ", Slave_IO_Running = " + slaveIORunning + ", Slave_SQL_Running = "
                            + slaveSQLRunning);
                return null;
            }

            String masterHost = maps.get("Master_Host");
            String masterPort = maps.get("Master_Port");
            String binlog = maps.get("Master_Log_File");
            String position = maps.get("Exec_Master_Log_Pos");
            return new SlaveEntryPosition(binlog, Long.valueOf(position), masterHost, masterPort);
        } catch (IOException e) {
            logger.error("find slave position error", e);
        }

        return null;
    }

    /**
     * 根据给定的时间戳，在指定的binlog中找到最接近于该时间戳(必须是小于时间戳)的一个事务起始位置。
     * 针对最后一个binlog会给定endPosition，避免无尽的查询
     */
    private EntryPosition findAsPerTimestampInSpecificLogFile(MysqlConnection mysqlConnection,
                                                              final Long startTimestamp,
                                                              final EntryPosition endPosition,
                                                              final String searchBinlogFile,
                                                              final Boolean justForPositionTimestamp) {

        final LogPosition logPosition = new LogPosition();
        try {
            mysqlConnection.reconnect();
            // 开始遍历文件
            mysqlConnection.seek(searchBinlogFile, 4L, endPosition.getGtid(), new SinkFunction<LogEvent>() {

                private LogPosition lastPosition;

                public boolean sink(LogEvent event) {
                    EntryPosition entryPosition = null;
                    try {
                        CanalEntry.Entry entry = parseAndProfilingIfNecessary(event, true);
                        if (justForPositionTimestamp && logPosition.getPostion() == null && event.getWhen() > 0) {
                            // 初始位点
                            entryPosition = new EntryPosition(searchBinlogFile,
                                event.getLogPos() - event.getEventLen(),
                                event.getWhen() * 1000,
                                event.getServerId());
                            entryPosition.setGtid(event.getHeader().getGtidSetStr());
                            logPosition.setPostion(entryPosition);
                        }

                        // 直接用event的位点来处理,解决一个binlog文件里没有任何事件导致死循环无法退出的问题
                        String logfilename = event.getHeader().getLogFileName();
                        // 记录的是binlog end offest,
                        // 因为与其对比的offest是show master status里的end offest
                        Long logfileoffset = event.getHeader().getLogPos();
                        Long logposTimestamp = event.getHeader().getWhen() * 1000;
                        Long serverId = event.getHeader().getServerId();

                        // 如果最小的一条记录都不满足条件，可直接退出
                        if (logposTimestamp >= startTimestamp) {
                            return false;
                        }

                        if (StringUtils.equals(endPosition.getJournalName(), logfilename)
                            && endPosition.getPosition() <= logfileoffset) {
                            return false;
                        }

                        if (entry == null) {
                            return true;
                        }

                        // 记录一下上一个事务结束的位置，即下一个事务的position
                        // position = current +
                        // data.length，代表该事务的下一条offest，避免多余的事务重复
                        if (CanalEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())) {
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp, serverId);
                            if (logger.isDebugEnabled()) {
                                logger.debug("set {} to be pending start position before finding another proper one...",
                                    entryPosition);
                            }
                            logPosition.setPostion(entryPosition);
                            entryPosition.setGtid(entry.getHeader().getGtid());
                        } else if (CanalEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType())) {
                            // 当前事务开始位点
                            entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp, serverId);
                            if (logger.isDebugEnabled()) {
                                logger.debug("set {} to be pending start position before finding another proper one...",
                                    entryPosition);
                            }
                            entryPosition.setGtid(entry.getHeader().getGtid());
                            logPosition.setPostion(entryPosition);
                        }

                        lastPosition = buildLastPosition(entry);
                    } catch (Throwable e) {
                        processSinkError(e, lastPosition, searchBinlogFile, 4L);
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

    @Override
    protected void processDumpError(Throwable e) {
        if (e instanceof IOException) {
            String message = e.getMessage();
            if (StringUtils.contains(message, "errno = 1236")) {
                // 1236 errorCode代表ER_MASTER_FATAL_ERROR_READING_BINLOG
                dumpErrorCount++;
            }
        }

        super.processDumpError(e);
    }

    public void setSupportBinlogFormats(String formatStrs) {
        String[] formats = StringUtils.split(formatStrs, ',');
        if (formats != null) {
            BinlogFormat[] supportBinlogFormats = new BinlogFormat[formats.length];
            int i = 0;
            for (String format : formats) {
                supportBinlogFormats[i++] = BinlogFormat.valuesOf(format);
            }

            this.supportBinlogFormats = supportBinlogFormats;
        }
    }

    public void setSupportBinlogImages(String imageStrs) {
        String[] images = StringUtils.split(imageStrs, ',');
        if (images != null) {
            BinlogImage[] supportBinlogImages = new BinlogImage[images.length];
            int i = 0;
            for (String image : images) {
                supportBinlogImages[i++] = BinlogImage.valuesOf(image);
            }

            this.supportBinlogImages = supportBinlogImages;
        }
    }

    // ===================== setter / getter ========================

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

    public void setFallbackIntervalInSeconds(int fallbackIntervalInSeconds) {
        this.fallbackIntervalInSeconds = fallbackIntervalInSeconds;
    }

    public CanalHAController getHaController() {
        return haController;
    }

    public void setHaController(CanalHAController haController) {
        this.haController = haController;
    }

    public void setDumpErrorCountThreshold(int dumpErrorCountThreshold) {
        this.dumpErrorCountThreshold = dumpErrorCountThreshold;
    }

    public boolean isRdsOssMode() {
        return rdsOssMode;
    }

    public void setRdsOssMode(boolean rdsOssMode) {
        this.rdsOssMode = rdsOssMode;
    }

    public void setDumpErrorCount(int dumpErrorCount) {
        this.dumpErrorCount = dumpErrorCount;
    }

    public boolean isAutoResetLatestPosMode() {
        return autoResetLatestPosMode;
    }

    public void setAutoResetLatestPosMode(boolean autoResetLatestPosMode) {
        this.autoResetLatestPosMode = autoResetLatestPosMode;
    }
}

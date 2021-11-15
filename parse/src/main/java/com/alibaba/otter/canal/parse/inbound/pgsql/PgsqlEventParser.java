package com.alibaba.otter.canal.parse.inbound.pgsql;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.PositionNotFoundException;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.taobao.tddl.dbsync.binlog.pgsql.PgsqlLogEvent;
import java.io.IOException;
import java.util.TimerTask;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.MDC;

/**
 * <pre>
 *   1. 构造连接connection
 *   2. 启动一个心跳线程
 *   3. 执行dump前的准备工作
 *   4. 获取最后的位置信息
 *   5. 构建StageProcessor
 *   6. 启动StageProcessor
 *   7. 根据是否并行(parallel)开启dump
 *        true: {@link PgsqlConnection#dump(long, MultiStageCoprocessor)} 或 {@link PgsqlConnection#dump(String, Long, MultiStageCoprocessor)}
 *        false: {@link PgsqlConnection#dump(long, SinkFunction)} 或 {@link PgsqlConnection#dump(String, Long, SinkFunction)}
 * </pre>
 */
public class PgsqlEventParser extends AbstractEventParser<PgsqlLogEvent> implements CanalHASwitchable {

  // 数据库信息
  protected AuthenticationInfo masterInfo;        // 主库
  protected AuthenticationInfo standbyInfo;       // 备库
  // binlog信息
  protected EntryPosition masterPosition;
  protected EntryPosition standbyPosition;
  private CanalHAController haController = null;
  // 连接信息
  private int connectTimeoutInSeconds = 10;         // connectTimeout
  private int loginTimeoutInSeconds = 10;           // loginTimeout
  private int socketTimeoutInSeconds = 10;          // socketTimeout
  private int receiveBufferSize = 64 * 1024 * 1024; // 64MB
  private int sendBufferSize = 64 * 1024 * 1024;    // 64MB

  private long slaveId;                           // 链接到pgsql的connection的id
  private PgsqlConnection dumpConnection;         // dumpConnection

  // 心跳检查信息
  private String detectingSQL;                    // 心跳sql
  private PgsqlConnection metaConnection;         // 查询meta信息的链接
  private int fallbackIntervalInSeconds = 60;     // 切换回退时间
  private int dumpErrorCount = 0;                 // binlogDump失败异常计数
  private int dumpErrorCountThreshold = 2;        // binlogDump失败异常计数阀值
  private boolean autoResetLatestPosMode = false; // true: binlog被删除之后，自动按最新的数据订阅
  // MetaCache
  private PgsqlMetaCache tableMetaCache;          // 对应meta
  private AviaterRegexFilter nameFilter;          // 名字过滤, schema.table
  private int metaCacheTimeout = 60;              // 元数据超时时间

  /**
   * {@link #transactionBuffer}的默认刷新机制是: 添加到{@link #eventSink}中,然后持久化position到zk中.
   *
   * @see AbstractEventParser#AbstractEventParser()
   */
  public PgsqlEventParser() {
    super.isGTIDMode = false;
    super.parallel = false;
    super.profilingEnabled.set(true);
  }

  @Override
  public synchronized void start() {
    if (runningInfo == null) {
      runningInfo = masterInfo;
    }
    if (this.running) {
      throw new CanalException(this.getClass().getName() + " has startup , don't repeat start");
    } else {
      this.running = true;
    }
    MDC.put("destination", destination);
    // 初始化 transaction buffer
    transactionBuffer.setBufferSize(transactionSize);// 设置buffer大小
    transactionBuffer.start();

    // 构造 binlog parser
    binlogParser = buildParser();// 初始化一下BinLogParser
    binlogParser.start();
    handler = (t, e) -> {
      Throwable err = ExceptionUtils.getRootCause(e);
      if (err instanceof InterruptedException) {
        parseThread.interrupt();
        running = false;
      }
      logger.error("dump events has an error", e);
    };
    // 启动 工作线程
    parseThread = new Thread(this::dumping);

    parseThread.setUncaughtExceptionHandler(handler);
    parseThread.setName(String.format("destination = %s,address = %s, DumpTask",
        destination, runningInfo == null ? null : runningInfo.getAddress()));
    parseThread.start();
  }

  @Override
  protected PgsqlBinlogParser buildParser() {
    PgsqlBinlogParser parser = new PgsqlBinlogParser();
    parser.setNameFilter(this.nameFilter);
    return parser;
  }

  @Override
  protected PgsqlConnection buildErosaConnection() {
    PgsqlConnection connection = new PgsqlJdbcConnection(runningInfo.getAddress().getHostName(),
        runningInfo.getAddress().getPort(),
        runningInfo.getUsername(), runningInfo.getPassword(), runningInfo.getDefaultDatabaseName());
    connection.setLoginTimeoutInSeconds(loginTimeoutInSeconds);
    connection.setSocketTimeoutInSeconds(socketTimeoutInSeconds);
    connection.setConnectTimeoutInSeconds(connectTimeoutInSeconds);
    connection.setReceiveBufferSize(receiveBufferSize);
    connection.setSendBufferSize(sendBufferSize);
    return connection;
  }

  @Override
  protected MultiStageCoprocessor buildMultiStageCoprocessor() {
    throw new IllegalArgumentException("unsupported now");
  }

  @Override
  protected EntryPosition findStartPosition(ErosaConnection connection) throws IOException {
    PgsqlConnection pgsqlConnection = (PgsqlConnection) connection;
    LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
    EntryPosition entryPosition;
    if (logPosition == null) {
      entryPosition = new EntryPosition(destination, 0L, System.currentTimeMillis());
    } else {
      entryPosition = logPosition.getPostion();
      if (!StringUtils.equalsIgnoreCase(entryPosition.getJournalName(), destination)) {
        entryPosition.setJournalName(destination);
      }
    }
    entryPosition.setServerId(pgsqlConnection.queryServerId());
    return entryPosition;
  }

  @Override
  protected LogPosition buildLastPosition(Entry entry) {
    EntryPosition position = new EntryPosition();
    position.setJournalName(destination);
    position.setPosition(entry.getHeader().getLogfileOffset());
    position.setTimestamp(entry.getHeader().getExecuteTime());
    position.setServerId(entry.getHeader().getServerId());
    position.setGtid(entry.getHeader().getGtid());// gtid= xid + "_" + lsn

    LogPosition logPosition = new LogPosition();
    logPosition.setPostion(position);
    logPosition.setIdentity(new LogIdentity(runningInfo.getAddress(), slaveId));
    return logPosition;
  }

  @Override
  public void doSwitch() {
    AuthenticationInfo info = this.runningInfo.equals(this.masterInfo) ? this.standbyInfo : this.masterInfo;
    doSwitch(info);
  }

  @Override
  public synchronized void doSwitch(AuthenticationInfo newInfo) {
    // 1. 停止当前复制
    // 2. 找到新的position点
    // 3. 建立新的链接,开启复制
    if (this.runningInfo.equals(newInfo)) {
      return;
    }
    if (newInfo == null) {
      sendAlarm(destination, "no configured standby database info, do nothing.");
      return;
    }
    stop();
    this.runningInfo = newInfo;
    start();
  }

  @Override
  protected void stopHeartBeat() {
    TimerTask heartBeatTimerTask = this.heartBeatTimerTask;
    super.stopHeartBeat();
    if (heartBeatTimerTask instanceof PgsqlDetectingTimeTask) {
      PgsqlConnection pgsqlConnection = ((PgsqlDetectingTimeTask) heartBeatTimerTask).pgsqlConnection;
      pgsqlConnection.disconnect();
    }
    logger.info("## stopped heartbeat...");
  }

  @Override
  protected TimerTask buildHeartBeatTimeTask(ErosaConnection connection) {
    if (!(connection instanceof PgsqlConnection)) {
      throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
    }
    // 开始pgsql的心跳sql Task
    if (detectingEnable) {
      return new PgsqlDetectingTimeTask((PgsqlConnection) connection.fork());
    } else {
      return super.buildHeartBeatTimeTask(connection);
    }
  }

  @Override
  protected void preDump(ErosaConnection connection) {
    if (!(connection instanceof PgsqlConnection)) {
      throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
    }
    if (this.binlogParser != null) {
      this.metaConnection = (PgsqlConnection) connection.fork();
      try {
        this.metaConnection.connect();
      } catch (IOException e) {
        throw new CanalParseException(e);
      }
      this.tableMetaCache = new PgsqlMetaCache(this.metaConnection, this.metaCacheTimeout);
      ((PgsqlBinlogParser) this.binlogParser).setTableMetaCache(this.tableMetaCache);
    }
  }

  @Override
  protected void afterDump(ErosaConnection connection) {
    if (connection == null) {
      throw new CanalParseException("illegal connection is null");
    }
    if (!(connection instanceof PgsqlConnection)) {
      throw new CanalParseException("Unsupported connection type : " + connection.getClass().getSimpleName());
    }
    if (this.metaConnection != null) {
      this.metaConnection.disconnect();
    }
    this.tableMetaCache.clear();
  }

  @Override
  public final void setIsGTIDMode(boolean isGTIDMode) {
    // pgsql没有gtid一说,直接禁用即可
  }

  @Override
  public void setParallel(boolean parallel) {

  }

  @Override
  public void setProfilingEnabled(boolean profilingEnabled) {
  }

  protected void dumping() {
    // 开始执行replication
    MDC.put("destination", String.valueOf(this.destination));
    while (this.running) {
      try {
        // 1. 构造PgsqlConnection连接
        this.dumpConnection = buildErosaConnection();

        // 2. 启动一个心跳线程
        startHeartBeat(this.dumpConnection);

        // 3. 执行dump前的准备工作
        preDump(this.dumpConnection);

        this.dumpConnection.connect();// 链接

        long queryServerId = this.dumpConnection.queryServerId();
        if (queryServerId != 0) {
          this.serverId = queryServerId;
        }
        if (this.dumpConnection.stat != null) {
          this.slaveId = this.dumpConnection.stat.getConnectionId();
        }

        // 4. 获取最后的位置信息
        long start = System.currentTimeMillis();
        EntryPosition startPosition = findStartPosition(this.dumpConnection);
        if (startPosition == null) {
          throw new PositionNotFoundException("can't find start position for " + this.destination);
        }

        if (!processTableMeta(startPosition)) {
          throw new CanalParseException("can't find init table meta for " + this.destination
              + " with position : " + startPosition);
        }
        long end = System.currentTimeMillis();
        logger.info("find start position successfully, {}", startPosition + " cost : "
            + (end - start) + "ms , the next step is binlog dump");

        SinkFunction<PgsqlLogEvent> sinkHandler = new SinkFunction<PgsqlLogEvent>() {

          private LogPosition lastPosition;

          @Override
          public boolean sink(PgsqlLogEvent event) {
            try {
              Entry entry = parseAndProfilingIfNecessary(event, false);

              if (!running) {
                return running;
              }

              if (entry != null) {
                // 有正常数据流过，清空exception
                exception = null;
                transactionBuffer.add(entry);
                // 记录一下对应的positions
                this.lastPosition = buildLastPosition(entry);
                // 记录一下最后一次有数据的时间
                lastEntryTime = System.currentTimeMillis();
              }
              return running;
            } catch (Throwable e) {
              // 记录一下，出错的位点信息
              processSinkError(e,
                  this.lastPosition,
                  startPosition.getJournalName(),
                  startPosition.getPosition());
              throw new CanalParseException(e); // 继续抛出异常，让上层统一感知
            }
          }

        };

        // 5. 开始dump数据
        logger.info("starting dumping.");
        dumpConnection.dump(startPosition.getJournalName(), startPosition.getPosition(), sinkHandler);
      } catch (Throwable e) {
        processDumpError(e);
        exception = e;
        if (!running) {
          throw new CanalParseException(String.format("dump address %s has an error, retrying. ",
              runningInfo.getAddress().toString()), e);
        } else {
          logger.error(String.format("dump address %s has an error, retrying. caused by ",
              runningInfo.getAddress().toString()), e);
          sendAlarm(destination, ExceptionUtils.getFullStackTrace(e));
        }
        if (parserExceptionHandler != null) {
          parserExceptionHandler.handle(e);
        }
        Throwable t = ExceptionUtils.getRootCause(e);
        if (t instanceof InterruptedException) {
          running = false;
          parseThread.interrupt();
          return;
        }
      } finally {
        // 重新置为中断状态
        boolean ignore = Thread.interrupted();
        // 关闭一下链接
        afterDump(this.dumpConnection);
        if (this.dumpConnection != null) {
          this.dumpConnection.disconnect();
          this.dumpConnection = null;
        }
      }
      // 出异常了，退出sink消费，释放一下状态
      eventSink.interrupt();
      transactionBuffer.reset();// 重置一下缓冲队列，重新记录数据
      binlogParser.reset();// 重新置位
      if (multiStageCoprocessor != null && multiStageCoprocessor.isStart()) {
        // 处理 RejectedExecutionException
        try {
          multiStageCoprocessor.stop();
        } catch (Throwable t) {
          logger.debug("multi processor rejected:", t);
        }
      }

      if (running) {
        // sleep一段时间再进行重试
        try {
          Thread.sleep(10000 + RandomUtils.nextInt(10000));
        } catch (InterruptedException ignore) {
        }
      }
    }
    MDC.remove("destination");
  }

  // region getter/setter

  public CanalHAController getHaController() {
    return haController;
  }

  public void setHaController(CanalHAController haController) {
    this.haController = haController;
  }

  public int getConnectTimeoutInSeconds() {
    return connectTimeoutInSeconds;
  }

  public void setConnectTimeoutInSeconds(int connectTimeoutInSeconds) {
    this.connectTimeoutInSeconds = connectTimeoutInSeconds;
  }

  public int getLoginTimeoutInSeconds() {
    return loginTimeoutInSeconds;
  }

  public void setLoginTimeoutInSeconds(int loginTimeoutInSeconds) {
    this.loginTimeoutInSeconds = loginTimeoutInSeconds;
  }

  public int getSocketTimeoutInSeconds() {
    return socketTimeoutInSeconds;
  }

  public void setSocketTimeoutInSeconds(int socketTimeoutInSeconds) {
    this.socketTimeoutInSeconds = socketTimeoutInSeconds;
  }

  public int getReceiveBufferSize() {
    return receiveBufferSize;
  }

  public void setReceiveBufferSize(int receiveBufferSize) {
    this.receiveBufferSize = receiveBufferSize;
  }

  public int getSendBufferSize() {
    return sendBufferSize;
  }

  public void setSendBufferSize(int sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
  }

  public AuthenticationInfo getMasterInfo() {
    return masterInfo;
  }

  public void setMasterInfo(AuthenticationInfo masterInfo) {
    this.masterInfo = masterInfo;
  }

  public AuthenticationInfo getStandbyInfo() {
    return standbyInfo;
  }

  public void setStandbyInfo(AuthenticationInfo standbyInfo) {
    this.standbyInfo = standbyInfo;
  }

  public EntryPosition getMasterPosition() {
    return masterPosition;
  }

  public void setMasterPosition(EntryPosition masterPosition) {
    this.masterPosition = masterPosition;
  }

  public EntryPosition getStandbyPosition() {
    return standbyPosition;
  }

  public void setStandbyPosition(EntryPosition standbyPosition) {
    this.standbyPosition = standbyPosition;
  }

  public long getSlaveId() {
    return slaveId;
  }

  public void setSlaveId(long slaveId) {
    this.slaveId = slaveId;
  }

  public PgsqlConnection getDumpConnection() {
    return dumpConnection;
  }

  public void setDumpConnection(PgsqlConnection dumpConnection) {
    this.dumpConnection = dumpConnection;
  }

  public String getDetectingSQL() {
    return detectingSQL;
  }

  public void setDetectingSQL(String detectingSQL) {
    this.detectingSQL = detectingSQL;
  }

  public PgsqlConnection getMetaConnection() {
    return metaConnection;
  }

  public void setMetaConnection(PgsqlConnection metaConnection) {
    this.metaConnection = metaConnection;
  }

  public int getFallbackIntervalInSeconds() {
    return fallbackIntervalInSeconds;
  }

  public void setFallbackIntervalInSeconds(int fallbackIntervalInSeconds) {
    this.fallbackIntervalInSeconds = fallbackIntervalInSeconds;
  }

  public int getDumpErrorCount() {
    return dumpErrorCount;
  }

  public void setDumpErrorCount(int dumpErrorCount) {
    this.dumpErrorCount = dumpErrorCount;
  }

  public int getDumpErrorCountThreshold() {
    return dumpErrorCountThreshold;
  }

  public void setDumpErrorCountThreshold(int dumpErrorCountThreshold) {
    this.dumpErrorCountThreshold = dumpErrorCountThreshold;
  }

  public boolean isAutoResetLatestPosMode() {
    return autoResetLatestPosMode;
  }

  public void setAutoResetLatestPosMode(boolean autoResetLatestPosMode) {
    this.autoResetLatestPosMode = autoResetLatestPosMode;
  }

  public PgsqlMetaCache getTableMetaCache() {
    return tableMetaCache;
  }

  public void setTableMetaCache(PgsqlMetaCache tableMetaCache) {
    this.tableMetaCache = tableMetaCache;
  }

  public AviaterRegexFilter getNameFilter() {
    return nameFilter;
  }

  public void setNameFilter(AviaterRegexFilter nameFilter) {
    this.nameFilter = nameFilter;
  }

  public int getMetaCacheTimeout() {
    return metaCacheTimeout;
  }

  public void setMetaCacheTimeout(int metaCacheTimeout) {
    this.metaCacheTimeout = metaCacheTimeout;
  }

  // endregion getter/setter

  class PgsqlDetectingTimeTask extends TimerTask {

    private final PgsqlConnection pgsqlConnection;
    private boolean reconnect = false;

    public PgsqlDetectingTimeTask(PgsqlConnection pgsqlConnection) {
      this.pgsqlConnection = pgsqlConnection;
    }

    @Override
    public void run() {
      try {
        if (reconnect) {
          reconnect = false;
          pgsqlConnection.reconnect();
        } else if (!pgsqlConnection.isConnected()) {
          pgsqlConnection.connect();
        }
        long startTime = System.currentTimeMillis();

        if (StringUtils.isBlank(detectingSQL)) {
          pgsqlConnection.isConnected();
        } else if (StringUtils.startsWithIgnoreCase(detectingSQL.trim(), "select")) {
          pgsqlConnection.query(detectingSQL);
        } else {
          pgsqlConnection.update(detectingSQL);
        }

        long costTime = System.currentTimeMillis() - startTime;
        if (haController != null && haController instanceof HeartBeatCallback) {
          ((HeartBeatCallback) haController).onSuccess(costTime);
        }
        logger.info("## heartbeat...");
      } catch (Throwable e) {
        if (haController != null && haController instanceof HeartBeatCallback) {
          ((HeartBeatCallback) haController).onFailed(e);
        }
        reconnect = true;
        logger.warn("## connect failed by ", e);
      }
    }
  }
}

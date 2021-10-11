package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.PositionNotFoundException;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.BinlogConnection;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DefaultTableMetaTSDBFactory;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDBFactory;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.exception.TableIdNotFoundException;

import static com.alibaba.otter.canal.parse.driver.mysql.utils.GtidUtil.parseGtidSet;

public abstract class AbstractMysqlEventParser extends AbstractEventParser<LogEvent> {

    protected static final long    BINLOG_START_OFFSET       = 4L;

    protected TableMetaTSDBFactory tableMetaTSDBFactory      = new DefaultTableMetaTSDBFactory();
    protected boolean              enableTsdb                = false;
    protected int                  tsdbSnapshotInterval      = 24;
    protected int                  tsdbSnapshotExpire        = 360;
    protected String               tsdbSpringXml;
    protected TableMetaTSDB        tableMetaTSDB;

    // 编码信息
    protected byte                 connectionCharsetNumber   = (byte) 33;

    protected boolean              filterRows                = false;
    protected boolean              useDruidDdlFilter         = true;

    // 是否跳过table相关的解析异常
    protected boolean              filterTableError           = false;

    protected AtomicBoolean        needTransactionPosition    = new AtomicBoolean(false);
    protected volatile boolean     detectingEnable            = true;                                    // 是否开启心跳检查
    protected Integer              detectingIntervalInSeconds = 3;                                       // 检测频率
    protected volatile Timer       timer;
    protected TimerTask            heartBeatTimerTask;

    protected boolean              isGTIDMode                 = false;                                   // 是否是GTID模式
    protected long                 serverId;

    protected abstract ErosaConnection buildErosaConnection();

    protected abstract EntryPosition findStartPosition(ErosaConnection connection) throws IOException;

    @Override
    protected AbstractBinlogParser<LogEvent> buildParser() {
        LogEventConvert convert = new LogEventConvert();
        convert.setCharset(connectionCharset);
        convert.setFilterRows(filterRows);
        convert.setFilterTableError(filterTableError);
        convert.setUseDruidDdlFilter(useDruidDdlFilter);
        return convert;
    }

    @Override
    public void setEventFilter(CanalEventFilter eventFilter) {
        super.setEventFilter(eventFilter);

        if (eventFilter instanceof AviaterRegexFilter) {
            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
            }
        }
    }

    @Override
    public void setEventBlackFilter(CanalEventFilter eventBlackFilter) {
        super.setEventBlackFilter(eventBlackFilter);

        if (eventBlackFilter instanceof AviaterRegexFilter) {
            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
            }
        }
    }

    @Override
    public void setFieldFilter(String fieldFilter) {
        super.setFieldFilter(fieldFilter);

        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setFieldFilterMap(getFieldFilterMap());
        }
    }

    @Override
    public void setFieldBlackFilter(String fieldBlackFilter) {
        super.setFieldBlackFilter(fieldBlackFilter);

        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setFieldBlackFilterMap(getFieldBlackFilterMap());
        }
    }

    @Override
    protected BinlogConnection buildBinlogConnection() {
        return buildErosaConnection();
    }

    @Override
    protected void dump(BinlogConnection connection) throws IOException {
        ErosaConnection erosaConnection = (ErosaConnection) connection;
        boolean isMariaDB = false;
        try {
            // 启动一个心跳线程
            startHeartBeat(erosaConnection);
            // 连接
            erosaConnection.connect();

            long queryServerId = erosaConnection.queryServerId();
            if (queryServerId != 0) {
                serverId = queryServerId;
            }

            if (erosaConnection instanceof MysqlConnection) {
                isMariaDB = ((MysqlConnection)erosaConnection).isMariaDB();
            }
            // 4. 获取最后的位置信息
            long start = System.currentTimeMillis();
            logger.warn("---> begin to find start position, it will be long time for reset or first position");
            final EntryPosition startPosition = findStartPosition(erosaConnection);
            if (startPosition == null) {
                throw new PositionNotFoundException("can't find start position for " + destination);
            }

            if (!processTableMeta(startPosition)) {
                throw new CanalParseException("can't find init table meta for " + destination
                                              + " with position : " + startPosition);
            }
            long end = System.currentTimeMillis();
            logger.warn("---> find start position successfully, {}", startPosition + " cost : "
                                                                     + (end - start)
                                                                     + "ms , the next step is binlog dump");
            // 重新链接，因为在找position过程中可能有状态，需要断开后重建
            erosaConnection.reconnect();

            final SinkFunction<LogEvent> sinkHandler = buildSinkHandler(startPosition);

            // 4. 开始dump数据
            if (parallel) {
                // build stage processor
                multiStageCoprocessor = buildMultiStageCoprocessor();
                if (isGTIDMode() && StringUtils.isNotEmpty(startPosition.getGtid())) {
                    // 判断所属instance是否启用GTID模式，是的话调用ErosaConnection中GTID对应方法dump数据
                    GTIDSet gtidSet = parseGtidSet(startPosition.getGtid(),isMariaDB);
                    ((MysqlMultiStageCoprocessor) multiStageCoprocessor).setGtidSet(gtidSet);
                    multiStageCoprocessor.start();
                    erosaConnection.dump(gtidSet, multiStageCoprocessor);
                } else {
                    multiStageCoprocessor.start();
                    if (StringUtils.isEmpty(startPosition.getJournalName())
                        && startPosition.getTimestamp() != null) {
                        erosaConnection.dump(startPosition.getTimestamp(), multiStageCoprocessor);
                    } else {
                        erosaConnection.dump(startPosition.getJournalName(),
                                startPosition.getPosition(),
                                multiStageCoprocessor);
                    }
                }
            } else {
                if (isGTIDMode() && StringUtils.isNotEmpty(startPosition.getGtid())) {
                    // 判断所属instance是否启用GTID模式，是的话调用ErosaConnection中GTID对应方法dump数据
                    erosaConnection.dump(parseGtidSet(startPosition.getGtid(), isMariaDB), sinkHandler);
                } else {
                    if (StringUtils.isEmpty(startPosition.getJournalName())
                        && startPosition.getTimestamp() != null) {
                        erosaConnection.dump(startPosition.getTimestamp(), sinkHandler);
                    } else {
                        erosaConnection.dump(startPosition.getJournalName(),
                                startPosition.getPosition(),
                                sinkHandler);
                    }
                }
            }
        } catch (TableIdNotFoundException e) {
            exception = e;
            // 特殊处理TableIdNotFound异常,出现这样的异常，一种可能就是起始的position是一个事务当中，导致tablemap
            // Event时间没解析过
            needTransactionPosition.compareAndSet(false, true);
            logger.error(String.format("dump address %s has an error, retrying. caused by ",
                    runningInfo.getAddress().toString()), e);
        }
    }

    @Override
    protected void processSinkError(Throwable e, EntryPosition startPosition, EntryPosition lastPosition) {
        if (lastPosition != null) {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s]", lastPosition),
                e);
            return;
        }
        if (startPosition != null) {
            logger.warn(String.format("ERROR ## parse this event has an error , last position : [%s,%s]",
                startPosition.getJournalName(),
                startPosition.getPosition()), e);
        }
    }

    protected void startHeartBeat(ErosaConnection connection) {
        lastEntryTime = 0L; // 初始化
        if (timer == null) {// lazy初始化一下
            String name = String.format("destination = %s , address = %s , HeartBeatTimeTask",
                    destination,
                    runningInfo == null ? null : runningInfo.getAddress().toString());
            synchronized (AbstractEventParser.class) {
                // synchronized (MysqlEventParser.class) {
                // why use MysqlEventParser.class, u know, MysqlEventParser is
                // the child class 4 AbstractEventParser,
                // do this is ...
                if (timer == null) {
                    timer = new Timer(name, true);
                }
            }
        }

        if (heartBeatTimerTask == null) {// fixed issue #56，避免重复创建heartbeat线程
            heartBeatTimerTask = buildHeartBeatTimeTask(connection);
            Integer interval = detectingIntervalInSeconds;
            timer.schedule(heartBeatTimerTask, interval * 1000L, interval * 1000L);
            logger.info("start heart beat.... ");
        }
    }

    protected TimerTask buildHeartBeatTimeTask(ErosaConnection connection) {
        return new TimerTask() {

            @Override
            public void run() {
                try {
                    if (exception == null || lastEntryTime > 0) {
                        // 如果未出现异常，或者有第一条正常数据
                        long now = System.currentTimeMillis();
                        long inteval = (now - lastEntryTime) / 1000;
                        if (inteval >= detectingIntervalInSeconds) {
                            CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
                            headerBuilder.setExecuteTime(now);
                            CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
                            entryBuilder.setHeader(headerBuilder.build());
                            entryBuilder.setEntryType(CanalEntry.EntryType.HEARTBEAT);
                            CanalEntry.Entry entry = entryBuilder.build();
                            // 提交到sink中，目前不会提交到store中，会在sink中进行忽略
                            consumeTheEventAndProfilingIfNecessary(Arrays.asList(entry));
                        }
                    }

                } catch (Throwable e) {
                    logger.warn("heartBeat run failed ", e);
                }
            }

        };
    }

    protected void stopHeartBeat() {
        lastEntryTime = 0L; // 初始化
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        heartBeatTimerTask = null;
    }

    /**
     * 回滚到指定位点
     *
     * @param position 当前的位点
     * @return 处理是否正常
     */
    protected boolean processTableMeta(EntryPosition position) {
        if (tableMetaTSDB != null) {
            if (position.getTimestamp() == null || position.getTimestamp() <= 0) {
                throw new CanalParseException("use gtid and TableMeta TSDB should be config timestamp > 0");
            }

            return tableMetaTSDB.rollback(position);
        }

        return true;
    }

    @Override
    public void start() throws CanalParseException {
        if (enableTsdb) {
            if (tableMetaTSDB == null) {
                synchronized (CanalEventParser.class) {
                    try {
                        // 设置当前正在加载的通道，加载spring查找文件时会用到该变量
                        System.setProperty("canal.instance.destination", destination);
                        // 初始化
                        tableMetaTSDB = tableMetaTSDBFactory.build(destination, tsdbSpringXml);
                    } finally {
                        System.setProperty("canal.instance.destination", "");
                    }
                }
            }
        }

        super.start();
    }

    @Override
    public void stop() throws CanalParseException {
        if (enableTsdb) {
            tableMetaTSDBFactory.destory(destination);
            tableMetaTSDB = null;
        }
        stopHeartBeat();
        super.stop();
    }

    @Override
    protected MultiStageCoprocessor buildMultiStageCoprocessor() {
        MysqlMultiStageCoprocessor mysqlMultiStageCoprocessor = new MysqlMultiStageCoprocessor(parallelBufferSize,
            parallelThreadSize,
            (LogEventConvert) binlogParser,
            transactionBuffer,
            destination, filterDmlInsert, filterDmlUpdate, filterDmlDelete);
        mysqlMultiStageCoprocessor.setEventsPublishBlockingTime(eventsPublishBlockingTime);
        return mysqlMultiStageCoprocessor;
    }

    // ============================ setter / getter =========================

    public void setConnectionCharsetNumber(byte connectionCharsetNumber) {
        this.connectionCharsetNumber = connectionCharsetNumber;
    }

    public void setFilterRows(boolean filterRows) {
        this.filterRows = filterRows;
    }

    public void setUseDruidDdlFilter(boolean useDruidDdlFilter) {
        this.useDruidDdlFilter = useDruidDdlFilter;
    }

    public void setFilterTableError(boolean filterTableError) {
        this.filterTableError = filterTableError;
    }

    public void setEnableTsdb(boolean enableTsdb) {
        this.enableTsdb = enableTsdb;
        if (this.enableTsdb) {
            if (tableMetaTSDB == null) {
                // 初始化
                tableMetaTSDB = tableMetaTSDBFactory.build(destination, tsdbSpringXml);
            }
        }
    }

    public void setTsdbSpringXml(String tsdbSpringXml) {
        this.tsdbSpringXml = tsdbSpringXml;
        if (this.enableTsdb) {
            if (tableMetaTSDB == null) {
                // 初始化
                tableMetaTSDB = tableMetaTSDBFactory.build(destination, tsdbSpringXml);
            }
        }
    }

    public void setTableMetaTSDBFactory(TableMetaTSDBFactory tableMetaTSDBFactory) {
        this.tableMetaTSDBFactory = tableMetaTSDBFactory;
    }

    public int getTsdbSnapshotInterval() {
        return tsdbSnapshotInterval;
    }

    public void setTsdbSnapshotInterval(int tsdbSnapshotInterval) {
        this.tsdbSnapshotInterval = tsdbSnapshotInterval;
    }

    public int getTsdbSnapshotExpire() {
        return tsdbSnapshotExpire;
    }

    public void setTsdbSnapshotExpire(int tsdbSnapshotExpire) {
        this.tsdbSnapshotExpire = tsdbSnapshotExpire;
    }

    public void setDetectingEnable(boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

    public void setDetectingIntervalInSeconds(Integer detectingIntervalInSeconds) {
        this.detectingIntervalInSeconds = detectingIntervalInSeconds;
    }

    public boolean isGTIDMode() {
        return isGTIDMode;
    }

    public void setIsGTIDMode(boolean isGTIDMode) {
        this.isGTIDMode = isGTIDMode;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }
}

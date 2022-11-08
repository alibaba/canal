package com.alibaba.otter.canal.parse.inbound.mysql;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DefaultTableMetaTSDBFactory;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDBFactory;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import org.apache.commons.lang.StringUtils;

public abstract class AbstractMysqlEventParser extends AbstractEventParser {

    protected static final long    BINLOG_START_OFFEST       = 4L;

    protected TableMetaTSDBFactory tableMetaTSDBFactory      = new DefaultTableMetaTSDBFactory();
    protected boolean              enableTsdb                = false;
    protected int                  tsdbSnapshotInterval      = 24;
    protected int                  tsdbSnapshotExpire        = 360;
    protected String               tsdbSpringXml;
    protected TableMetaTSDB        tableMetaTSDB;

    // 编码信息
    protected byte                 connectionCharsetNumber   = (byte) 33;
    protected Charset              connectionCharset         = Charset.forName("UTF-8");
    protected boolean              filterQueryDcl            = false;
    protected boolean              filterQueryDml            = false;
    protected boolean              filterQueryDdl            = false;
    protected boolean              filterRows                = false;
    protected boolean              filterTableError          = false;
    protected boolean              useDruidDdlFilter         = true;

    protected boolean              filterDmlInsert           = false;
    protected boolean              filterDmlUpdate           = false;
    protected boolean              filterDmlDelete           = false;
    // instance received binlog bytes
    protected final AtomicLong     receivedBinlogBytes       = new AtomicLong(0L);
    private final AtomicLong       eventsPublishBlockingTime = new AtomicLong(0L);

    protected BinlogParser buildParser() {
        LogEventConvert convert = new LogEventConvert();
        if (eventFilter != null && eventFilter instanceof AviaterRegexFilter) {
            convert.setNameFilter((AviaterRegexFilter) eventFilter);
        }

        if (eventBlackFilter != null && eventBlackFilter instanceof AviaterRegexFilter) {
            convert.setNameBlackFilter((AviaterRegexFilter) eventBlackFilter);
        }

        convert.setFieldFilterMap(getFieldFilterMap());
        convert.setFieldBlackFilterMap(getFieldBlackFilterMap());

        convert.setCharset(connectionCharset);
        convert.setFilterQueryDcl(filterQueryDcl);
        convert.setFilterQueryDml(filterQueryDml);
        convert.setFilterQueryDdl(filterQueryDdl);
        convert.setFilterRows(filterRows);
        convert.setFilterTableError(filterTableError);
        convert.setUseDruidDdlFilter(useDruidDdlFilter);
        return convert;
    }

    public void setEventFilter(CanalEventFilter eventFilter) {
        super.setEventFilter(eventFilter);

        // 触发一下filter变更
        if (eventFilter != null && eventFilter instanceof AviaterRegexFilter) {
            if (binlogParser instanceof LogEventConvert) {
                ((LogEventConvert) binlogParser).setNameFilter((AviaterRegexFilter) eventFilter);
            }

            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
            }
        }
    }

    public void setEventBlackFilter(CanalEventFilter eventBlackFilter) {
        super.setEventBlackFilter(eventBlackFilter);

        // 触发一下filter变更
        if (eventBlackFilter != null && eventBlackFilter instanceof AviaterRegexFilter) {
            if (binlogParser instanceof LogEventConvert) {
                ((LogEventConvert) binlogParser).setNameBlackFilter((AviaterRegexFilter) eventBlackFilter);
            }

            if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
                ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
            }
        }
    }

    @Override
    public void setFieldFilter(String fieldFilter) {
        super.setFieldFilter(fieldFilter);

        // 触发一下filter变更
        if (binlogParser instanceof LogEventConvert) {
            ((LogEventConvert) binlogParser).setFieldFilterMap(getFieldFilterMap());
        }

        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setFieldFilterMap(getFieldFilterMap());
        }
    }

    @Override
    public void setFieldBlackFilter(String fieldBlackFilter) {
        super.setFieldBlackFilter(fieldBlackFilter);

        // 触发一下filter变更
        if (binlogParser instanceof LogEventConvert) {
            ((LogEventConvert) binlogParser).setFieldBlackFilterMap(getFieldBlackFilterMap());
        }

        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setFieldBlackFilterMap(getFieldBlackFilterMap());
        }
    }

    /**
     * 回滚到指定位点
     * 
     * @param position
     * @return
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

    public void stop() throws CanalParseException {
        if (enableTsdb) {
            tableMetaTSDBFactory.destory(destination);
            tableMetaTSDB = null;
        }

        super.stop();
    }

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

    public void setConnectionCharsetStd(Charset connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

    public void setConnectionCharset(String connectionCharset) {
        if ("UTF8MB4".equalsIgnoreCase(connectionCharset)) {
            connectionCharset = "UTF-8";
        }

        this.connectionCharset = Charset.forName(connectionCharset);
    }

    public void setFilterQueryDcl(boolean filterQueryDcl) {
        this.filterQueryDcl = filterQueryDcl;
    }

    public void setFilterQueryDml(boolean filterQueryDml) {
        this.filterQueryDml = filterQueryDml;
    }

    public void setFilterQueryDdl(boolean filterQueryDdl) {
        this.filterQueryDdl = filterQueryDdl;
    }

    public void setFilterRows(boolean filterRows) {
        this.filterRows = filterRows;
    }

    public void setFilterTableError(boolean filterTableError) {
        this.filterTableError = filterTableError;
    }

    public boolean isUseDruidDdlFilter() {
        return useDruidDdlFilter;
    }

    public void setUseDruidDdlFilter(boolean useDruidDdlFilter) {
        this.useDruidDdlFilter = useDruidDdlFilter;
    }

    public boolean isFilterDmlInsert() {
        return filterDmlInsert;
    }

    public void setFilterDmlInsert(boolean filterDmlInsert) {
        this.filterDmlInsert = filterDmlInsert;
    }

    public boolean isFilterDmlUpdate() {
        return filterDmlUpdate;
    }

    public void setFilterDmlUpdate(boolean filterDmlUpdate) {
        this.filterDmlUpdate = filterDmlUpdate;
    }

    public boolean isFilterDmlDelete() {
        return filterDmlDelete;
    }

    public void setFilterDmlDelete(boolean filterDmlDelete) {
        this.filterDmlDelete = filterDmlDelete;
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

    public AtomicLong getEventsPublishBlockingTime() {
        return this.eventsPublishBlockingTime;
    }

    public AtomicLong getReceivedBinlogBytes() {
        return this.receivedBinlogBytes;
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

}

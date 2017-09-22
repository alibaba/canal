package com.alibaba.otter.canal.parse.inbound.mysql;

import java.nio.charset.Charset;

import javax.annotation.Resource;

import com.taobao.tddl.dbsync.binlog.BinlogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaManager;

public abstract class AbstractMysqlEventParser extends AbstractEventParser {

    protected final Logger      logger                  = LoggerFactory.getLogger(this.getClass());
    protected static final long BINLOG_START_OFFEST     = 4L;

    // 编码信息
    protected byte              connectionCharsetNumber = (byte) 33;
    protected Charset           connectionCharset       = Charset.forName("UTF-8");
    protected boolean           filterQueryDcl          = false;
    protected boolean           filterQueryDml          = false;
    protected boolean           filterQueryDdl          = false;
    protected boolean           filterRows              = false;
    protected boolean           filterTableError        = false;

    @Resource
    protected TableMetaManager tableMetaManager;
    protected boolean useDruidDdlFilter = true;

    protected BinlogParser buildParser() {
        LogEventConvert convert = new LogEventConvert();
        if (eventFilter != null && eventFilter instanceof AviaterRegexFilter) {
            convert.setNameFilter((AviaterRegexFilter) eventFilter);
        }

        if (eventBlackFilter != null && eventBlackFilter instanceof AviaterRegexFilter) {
            convert.setNameBlackFilter((AviaterRegexFilter) eventBlackFilter);
        }

        convert.setCharset(connectionCharset);
        convert.setFilterQueryDcl(filterQueryDcl);
        convert.setFilterQueryDml(filterQueryDml);
        convert.setFilterQueryDdl(filterQueryDdl);
        convert.setFilterRows(filterRows);
        convert.setFilterTableError(filterTableError);

        //初始化parser的时候也初始化管理mysql 表结构的管理器
        tableMetaManager.init();
        return convert;
    }

    public void setEventFilter(CanalEventFilter eventFilter) {
        super.setEventFilter(eventFilter);

        // 触发一下filter变更
        if (eventFilter != null && eventFilter instanceof AviaterRegexFilter && binlogParser instanceof LogEventConvert) {
            ((LogEventConvert) binlogParser).setNameFilter((AviaterRegexFilter) eventFilter);
        }
    }

    /**
     * 回滚到指定位点
     * @param position
     * @return
     */
    protected boolean processTableMeta(BinlogPosition position) {
        if (tableMetaManager != null) {
            return tableMetaManager.rollback(position);
        }

        return true;
    }


    public void setEventBlackFilter(CanalEventFilter eventBlackFilter) {
        super.setEventBlackFilter(eventBlackFilter);

        // 触发一下filter变更
        if (eventBlackFilter != null && eventBlackFilter instanceof AviaterRegexFilter
            && binlogParser instanceof LogEventConvert) {
            ((LogEventConvert) binlogParser).setNameBlackFilter((AviaterRegexFilter) eventBlackFilter);
        }
    }

    // ============================ setter / getter =========================

    public void setConnectionCharsetNumber(byte connectionCharsetNumber) {
        this.connectionCharsetNumber = connectionCharsetNumber;
    }

    public void setConnectionCharset(Charset connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

    public void setConnectionCharset(String connectionCharset) {
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

    public TableMetaManager getTableMetaManager() {
        return tableMetaManager;
    }

    public void setTableMetaManager(TableMetaManager tableMetaManager) {
        this.tableMetaManager = tableMetaManager;
    }

    public boolean isUseDruidDdlFilter() {
        return useDruidDdlFilter;
    }

    public void setUseDruidDdlFilter(boolean useDruidDdlFilter) {
        this.useDruidDdlFilter = useDruidDdlFilter;
    }
}

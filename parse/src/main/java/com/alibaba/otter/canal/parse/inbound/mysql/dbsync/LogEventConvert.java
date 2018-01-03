package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.exception.TableIdNotFoundException;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.SimpleDdlParser;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.CanalEntry.Pair;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionBegin;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionEnd;
import com.alibaba.otter.canal.protocol.CanalEntry.Type;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.google.protobuf.ByteString;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.IntvarLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RandLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer;
import com.taobao.tddl.dbsync.binlog.event.RowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsQueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent.ColumnInfo;
import com.taobao.tddl.dbsync.binlog.event.UnknownLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UserVarLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.XidLogEvent;
import com.taobao.tddl.dbsync.binlog.event.mariadb.AnnotateRowsEvent;

/**
 * 基于{@linkplain LogEvent}转化为Entry对象的处理
 * 
 * @author jianghang 2013-1-17 下午02:41:14
 * @version 1.0.0
 */
public class LogEventConvert extends AbstractCanalLifeCycle implements BinlogParser<LogEvent> {

    public static final String          ISO_8859_1          = "ISO-8859-1";
    public static final String          UTF_8               = "UTF-8";
    public static final int             TINYINT_MAX_VALUE   = 256;
    public static final int             SMALLINT_MAX_VALUE  = 65536;
    public static final int             MEDIUMINT_MAX_VALUE = 16777216;
    public static final long            INTEGER_MAX_VALUE   = 4294967296L;
    public static final BigInteger      BIGINT_MAX_VALUE    = new BigInteger("18446744073709551616");
    public static final int             version             = 1;
    public static final String          BEGIN               = "BEGIN";
    public static final String          COMMIT              = "COMMIT";
    public static final Logger          logger              = LoggerFactory.getLogger(LogEventConvert.class);

    private volatile AviaterRegexFilter nameFilter;                                                          // 运行时引用可能会有变化，比如规则发生变化时
    private volatile AviaterRegexFilter nameBlackFilter;

    private TableMetaCache              tableMetaCache;
    private String                      binlogFileName      = "mysql-bin.000001";
    private Charset                     charset             = Charset.defaultCharset();
    private boolean                     filterQueryDcl      = false;
    private boolean                     filterQueryDml      = false;
    private boolean                     filterQueryDdl      = false;
    // 是否跳过table相关的解析异常,比如表不存在或者列数量不匹配,issue 92
    private boolean                     filterTableError    = false;
    // 新增rows过滤，用于仅订阅除rows以外的数据
    private boolean                     filterRows          = false;
    private boolean                     useDruidDdlFilter   = true;

    @Override
    public Entry parse(LogEvent logEvent, boolean isSeek) throws CanalParseException {
        if (logEvent == null || logEvent instanceof UnknownLogEvent) {
            return null;
        }

        int eventType = logEvent.getHeader().getType();
        switch (eventType) {
            case LogEvent.ROTATE_EVENT:
                binlogFileName = ((RotateLogEvent) logEvent).getFilename();
                break;
            case LogEvent.QUERY_EVENT:
                return parseQueryEvent((QueryLogEvent) logEvent, isSeek);
            case LogEvent.XID_EVENT:
                return parseXidEvent((XidLogEvent) logEvent);
            case LogEvent.TABLE_MAP_EVENT:
                break;
            case LogEvent.WRITE_ROWS_EVENT_V1:
            case LogEvent.WRITE_ROWS_EVENT:
                return parseRowsEvent((WriteRowsLogEvent) logEvent);
            case LogEvent.UPDATE_ROWS_EVENT_V1:
            case LogEvent.UPDATE_ROWS_EVENT:
                return parseRowsEvent((UpdateRowsLogEvent) logEvent);
            case LogEvent.DELETE_ROWS_EVENT_V1:
            case LogEvent.DELETE_ROWS_EVENT:
                return parseRowsEvent((DeleteRowsLogEvent) logEvent);
            case LogEvent.ROWS_QUERY_LOG_EVENT:
                return parseRowsQueryEvent((RowsQueryLogEvent) logEvent);
            case LogEvent.ANNOTATE_ROWS_EVENT:
                return parseAnnotateRowsEvent((AnnotateRowsEvent) logEvent);
            case LogEvent.USER_VAR_EVENT:
                return parseUserVarLogEvent((UserVarLogEvent) logEvent);
            case LogEvent.INTVAR_EVENT:
                return parseIntrvarLogEvent((IntvarLogEvent) logEvent);
            case LogEvent.RAND_EVENT:
                return parseRandLogEvent((RandLogEvent) logEvent);
            default:
                break;
        }

        return null;
    }

    public void reset() {
        // do nothing
        binlogFileName = "mysql-bin.000001";
        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }
    }

    private Entry parseQueryEvent(QueryLogEvent event, boolean isSeek) {
        String queryString = event.getQuery();
        if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            TransactionBegin transactionBegin = createTransactionBegin(event.getSessionId());
            Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, EntryType.TRANSACTIONBEGIN, transactionBegin.toByteString());
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            TransactionEnd transactionEnd = createTransactionEnd(0L); // MyISAM可能不会有xid事件
            Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());
        } else {
            boolean notFilter = false;
            EventType type = EventType.QUERY;
            String tableName = null;
            String schemaName = null;
            if (useDruidDdlFilter) {
                List<DdlResult> results = DruidDdlParser.parse(queryString, event.getDbName());
                for (DdlResult result : results) {
                    if (!processFilter(queryString, result)) {
                        // 只要有一个数据不进行过滤
                        notFilter = true;
                    }
                }
                if (results.size() > 0) {
                    // 如果针对多行的DDL,只能取第一条
                    type = results.get(0).getType();
                    schemaName = results.get(0).getSchemaName();
                    tableName = results.get(0).getTableName();
                }
            } else {
                DdlResult result = SimpleDdlParser.parse(queryString, event.getDbName());
                if (!processFilter(queryString, result)) {
                    notFilter = true;
                }

                type = result.getType();
                schemaName = result.getSchemaName();
                tableName = result.getTableName();
            }

            if (!notFilter) {
                // 如果是过滤的数据就不处理了
                return null;
            }

            if (!isSeek) {
                // 使用新的表结构元数据管理方式
                EntryPosition position = createPosition(event.getHeader());
                tableMetaCache.apply(position, event.getDbName(), queryString, null);
            }

            Header header = createHeader(binlogFileName, event.getHeader(), schemaName, tableName, type);
            RowChange.Builder rowChangeBuider = RowChange.newBuilder();
            if (type != EventType.QUERY) {
                rowChangeBuider.setIsDdl(true);
            }
            rowChangeBuider.setSql(queryString);
            if (StringUtils.isNotEmpty(event.getDbName())) {// 可能为空
                rowChangeBuider.setDdlSchemaName(event.getDbName());
            }
            rowChangeBuider.setEventType(type);
            return createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
        }
    }

    private boolean processFilter(String queryString, DdlResult result) {
        String schemaName = result.getSchemaName();
        String tableName = result.getTableName();
        // fixed issue https://github.com/alibaba/canal/issues/58
        // 更新下table meta cache
        if (tableMetaCache != null
            && (result.getType() == EventType.ALTER || result.getType() == EventType.ERASE || result.getType() == EventType.RENAME)) {
            // 对外返回，保证兼容，还是返回QUERY类型，这里暂不解析tableName，所以无法支持过滤
            for (DdlResult renameResult = result; renameResult != null; renameResult = renameResult.getRenameTableResult()) {
                String schemaName0 = renameResult.getSchemaName();
                String tableName0 = renameResult.getTableName();
                if (StringUtils.isNotEmpty(tableName0)) {
                    // 如果解析到了正确的表信息，则根据全名进行清除
                    tableMetaCache.clearTableMeta(schemaName0, tableName0);
                } else {
                    // 如果无法解析正确的表信息，则根据schema进行清除
                    tableMetaCache.clearTableMetaWithSchemaName(schemaName0);
                }
            }
        }

        // fixed issue https://github.com/alibaba/canal/issues/58
        if (result.getType() == EventType.ALTER || result.getType() == EventType.ERASE
            || result.getType() == EventType.CREATE || result.getType() == EventType.TRUNCATE
            || result.getType() == EventType.RENAME || result.getType() == EventType.CINDEX
            || result.getType() == EventType.DINDEX) { // 针对DDL类型

            if (filterQueryDdl) {
                return true;
            }

            if (StringUtils.isEmpty(tableName)
                || (result.getType() == EventType.RENAME && StringUtils.isEmpty(result.getOriTableName()))) {
                // 如果解析不出tableName,记录一下日志，方便bugfix，目前直接抛出异常，中断解析
                throw new CanalParseException("SimpleDdlParser process query failed. pls submit issue with this queryString: "
                                              + queryString + " , and DdlResult: " + result.toString());
                // return null;
            } else {
                // check name filter
                String name = schemaName + "." + tableName;
                if (nameFilter != null && !nameFilter.filter(name)) {
                    if (result.getType() == EventType.RENAME) {
                        // rename校验只要源和目标满足一个就进行操作
                        if (nameFilter != null
                            && !nameFilter.filter(result.getOriSchemaName() + "." + result.getOriTableName())) {
                            return true;
                        }
                    } else {
                        // 其他情况返回null
                        return true;
                    }
                }

                if (nameBlackFilter != null && nameBlackFilter.filter(name)) {
                    if (result.getType() == EventType.RENAME) {
                        // rename校验只要源和目标满足一个就进行操作
                        if (nameBlackFilter != null
                            && nameBlackFilter.filter(result.getOriSchemaName() + "." + result.getOriTableName())) {
                            return true;
                        }
                    } else {
                        // 其他情况返回null
                        return true;
                    }
                }
            }
        } else if (result.getType() == EventType.INSERT || result.getType() == EventType.UPDATE
                   || result.getType() == EventType.DELETE) {
            // 对外返回，保证兼容，还是返回QUERY类型，这里暂不解析tableName，所以无法支持过滤
            if (filterQueryDml) {
                return true;
            }
        } else if (filterQueryDcl) {
            return true;
        }

        return false;
    }

    private Entry parseRowsQueryEvent(RowsQueryLogEvent event) {
        if (filterQueryDml) {
            return null;
        }
        // mysql5.6支持，需要设置binlog-rows-query-log-events=1，可详细打印原始DML语句
        String queryString = null;
        try {
            queryString = new String(event.getRowsQuery().getBytes(ISO_8859_1), charset.name());
            String tableName = null;
            if (useDruidDdlFilter) {
                List<DdlResult> results = DruidDdlParser.parse(queryString, null);
                if (results.size() > 0) {
                    tableName = results.get(0).getTableName();
                }
            }

            return buildQueryEntry(queryString, event.getHeader(), tableName);
        } catch (UnsupportedEncodingException e) {
            throw new CanalParseException(e);
        }
    }

    private Entry parseAnnotateRowsEvent(AnnotateRowsEvent event) {
        if (filterQueryDml) {
            return null;
        }
        // mariaDb支持，需要设置binlog_annotate_row_events=true，可详细打印原始DML语句
        String queryString = null;
        try {
            queryString = new String(event.getRowsQuery().getBytes(ISO_8859_1), charset.name());
            return buildQueryEntry(queryString, event.getHeader());
        } catch (UnsupportedEncodingException e) {
            throw new CanalParseException(e);
        }
    }

    private Entry parseUserVarLogEvent(UserVarLogEvent event) {
        if (filterQueryDml) {
            return null;
        }

        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private Entry parseIntrvarLogEvent(IntvarLogEvent event) {
        if (filterQueryDml) {
            return null;
        }

        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private Entry parseRandLogEvent(RandLogEvent event) {
        if (filterQueryDml) {
            return null;
        }

        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private Entry parseXidEvent(XidLogEvent event) {
        TransactionEnd transactionEnd = createTransactionEnd(event.getXid());
        Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
        return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());
    }

    private Entry parseRowsEvent(RowsLogEvent event) {
        if (filterRows) {
            return null;
        }
        try {
            TableMapLogEvent table = event.getTable();
            if (table == null) {
                // tableId对应的记录不存在
                throw new TableIdNotFoundException("not found tableId:" + event.getTableId());
            }

            boolean isHeartBeat = isAliSQLHeartBeat(table.getDbName(), table.getTableName());
            boolean isRDSHeartBeat = tableMetaCache.isOnRDS()
                                     && isRDSHeartBeat(table.getDbName(), table.getTableName());

            String fullname = table.getDbName() + "." + table.getTableName();
            // check name filter
            if (nameFilter != null && !nameFilter.filter(fullname)) {
                return null;
            }
            if (nameBlackFilter != null && nameBlackFilter.filter(fullname)) {
                return null;
            }

            // if (isHeartBeat || isRDSHeartBeat) {
            // // 忽略rds模式的mysql.ha_health_check心跳数据
            // return null;
            // }
            TableMeta tableMeta = null;
            if (isRDSHeartBeat) {
                // 处理rds模式的mysql.ha_health_check心跳数据
                // 主要RDS的心跳表基本无权限,需要mock一个tableMeta
                FieldMeta idMeta = new FieldMeta("id", "bigint(20)", true, false, "0");
                FieldMeta typeMeta = new FieldMeta("type", "char(1)", false, true, "0");
                tableMeta = new TableMeta(table.getDbName(), table.getTableName(), Arrays.asList(idMeta, typeMeta));
            } else if (isHeartBeat) {
                // 处理alisql模式的test.heartbeat心跳数据
                // 心跳表基本无权限,需要mock一个tableMeta
                FieldMeta idMeta = new FieldMeta("id", "smallint(6)", false, true, null);
                FieldMeta typeMeta = new FieldMeta("type", "int(11)", true, false, null);
                tableMeta = new TableMeta(table.getDbName(), table.getTableName(), Arrays.asList(idMeta, typeMeta));
            }

            EventType eventType = null;
            int type = event.getHeader().getType();
            if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
                eventType = EventType.INSERT;
            } else if (LogEvent.UPDATE_ROWS_EVENT_V1 == type || LogEvent.UPDATE_ROWS_EVENT == type) {
                eventType = EventType.UPDATE;
            } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
                eventType = EventType.DELETE;
            } else {
                throw new CanalParseException("unsupport event type :" + event.getHeader().getType());
            }

            Header header = createHeader(binlogFileName,
                event.getHeader(),
                table.getDbName(),
                table.getTableName(),
                eventType);
            EntryPosition position = createPosition(event.getHeader());
            RowChange.Builder rowChangeBuider = RowChange.newBuilder();
            rowChangeBuider.setTableId(event.getTableId());
            rowChangeBuider.setIsDdl(false);

            rowChangeBuider.setEventType(eventType);
            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getChangeColumns();
            boolean tableError = false;
            if (tableMetaCache != null && tableMeta == null) {// 入错存在table meta
                                                              // cache
                tableMeta = getTableMeta(table.getDbName(), table.getTableName(), true, position);
                if (tableMeta == null) {
                    tableError = true;
                    if (!filterTableError) {
                        throw new CanalParseException("not found [" + fullname + "] in db , pls check!");
                    }
                }
            }

            while (buffer.nextOneRow(columns)) {
                // 处理row记录
                RowData.Builder rowDataBuilder = RowData.newBuilder();
                if (EventType.INSERT == eventType) {
                    // insert的记录放在before字段中
                    tableError |= parseOneRow(rowDataBuilder, event, buffer, columns, true, tableMeta);
                } else if (EventType.DELETE == eventType) {
                    // delete的记录放在before字段中
                    tableError |= parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta);
                } else {
                    // update需要处理before/after
                    tableError |= parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta);
                    if (!buffer.nextOneRow(changeColumns)) {
                        rowChangeBuider.addRowDatas(rowDataBuilder.build());
                        break;
                    }

                    tableError |= parseOneRow(rowDataBuilder, event, buffer, changeColumns, true, tableMeta);
                }

                rowChangeBuider.addRowDatas(rowDataBuilder.build());
            }

            RowChange rowChange = rowChangeBuider.build();
            if (tableError) {
                Entry entry = createEntry(header, EntryType.ROWDATA, ByteString.EMPTY);
                logger.warn("table parser error : {}storeValue: {}", entry.toString(), rowChange.toString());
                return null;
            } else {
                Entry entry = createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
                return entry;
            }
        } catch (Exception e) {
            throw new CanalParseException("parse row data failed.", e);
        }
    }

    private EntryPosition createPosition(LogHeader logHeader) {
        return new EntryPosition(binlogFileName,
            logHeader.getLogPos(),
            logHeader.getWhen() * 1000L,
            logHeader.getServerId()); // 记录到秒
    }

    private boolean parseOneRow(RowData.Builder rowDataBuilder, RowsLogEvent event, RowsLogBuffer buffer, BitSet cols,
                                boolean isAfter, TableMeta tableMeta) throws UnsupportedEncodingException {
        int columnCnt = event.getTable().getColumnCnt();
        ColumnInfo[] columnInfo = event.getTable().getColumnInfo();

        boolean tableError = false;
        // check table fileds count，只能处理加字段
        boolean existRDSNoPrimaryKey = false;
        if (tableMeta != null && columnInfo.length > tableMeta.getFields().size()) {
            if (tableMetaCache.isOnRDS()) {
                // 特殊处理下RDS的场景
                List<FieldMeta> primaryKeys = tableMeta.getPrimaryFields();
                if (primaryKeys == null || primaryKeys.isEmpty()) {
                    if (columnInfo.length == tableMeta.getFields().size() + 1
                        && columnInfo[columnInfo.length - 1].type == LogEvent.MYSQL_TYPE_LONGLONG) {
                        existRDSNoPrimaryKey = true;
                    }
                }
            }

            EntryPosition position = createPosition(event.getHeader());
            if (!existRDSNoPrimaryKey) {
                // online ddl增加字段操作步骤：
                // 1. 新增一张临时表，将需要做ddl表的数据全量导入
                // 2. 在老表上建立I/U/D的trigger，增量的将数据插入到临时表
                // 3. 锁住应用请求，将临时表rename为老表的名字，完成增加字段的操作
                // 尝试做一次reload，可能因为ddl没有正确解析，或者使用了类似online ddl的操作
                // 因为online ddl没有对应表名的alter语法，所以不会有clear cache的操作
                tableMeta = getTableMeta(event.getTable().getDbName(), event.getTable().getTableName(), false, position);// 强制重新获取一次
                if (tableMeta == null) {
                    tableError = true;
                    if (!filterTableError) {
                        throw new CanalParseException("not found [" + event.getTable().getDbName() + "."
                                                      + event.getTable().getTableName() + "] in db , pls check!");
                    }
                }

                // 在做一次判断
                if (tableMeta != null && columnInfo.length > tableMeta.getFields().size()) {
                    tableError = true;
                    if (!filterTableError) {
                        throw new CanalParseException("column size is not match for table:" + tableMeta.getFullName()
                                                      + "," + columnInfo.length + " vs " + tableMeta.getFields().size());
                    }
                }
            } else {
                logger.warn("[" + event.getTable().getDbName() + "." + event.getTable().getTableName()
                            + "] is no primary key , skip alibaba_rds_row_id column");
            }
        }

        for (int i = 0; i < columnCnt; i++) {
            ColumnInfo info = columnInfo[i];
            // mysql 5.6开始支持nolob/mininal类型,并不一定记录所有的列,需要进行判断
            if (!cols.get(i)) {
                continue;
            }

            if (existRDSNoPrimaryKey && i == columnCnt - 1 && info.type == LogEvent.MYSQL_TYPE_LONGLONG) {
                // 不解析最后一列
                buffer.nextValue(info.type, info.meta, false);
                continue;
            }

            Column.Builder columnBuilder = Column.newBuilder();

            FieldMeta fieldMeta = null;
            if (tableMeta != null && !tableError) {
                // 处理file meta
                fieldMeta = tableMeta.getFields().get(i);
                columnBuilder.setName(fieldMeta.getColumnName());
                columnBuilder.setIsKey(fieldMeta.isKey());
                // 增加mysql type类型,issue 73
                columnBuilder.setMysqlType(fieldMeta.getColumnType());
            }
            columnBuilder.setIndex(i);
            columnBuilder.setIsNull(false);

            // fixed issue
            // https://github.com/alibaba/canal/issues/66，特殊处理binary/varbinary，不能做编码处理
            boolean isBinary = false;
            if (fieldMeta != null) {
                if (StringUtils.containsIgnoreCase(fieldMeta.getColumnType(), "VARBINARY")) {
                    isBinary = true;
                } else if (StringUtils.containsIgnoreCase(fieldMeta.getColumnType(), "BINARY")) {
                    isBinary = true;
                }
            }
            buffer.nextValue(info.type, info.meta, isBinary);
            if (existRDSNoPrimaryKey && i == columnCnt - 1 && info.type == LogEvent.MYSQL_TYPE_LONGLONG) {
                // 不解析最后一列
                continue;
            }

            int javaType = buffer.getJavaType();
            if (buffer.isNull()) {
                columnBuilder.setIsNull(true);
            } else {
                final Serializable value = buffer.getValue();
                // 处理各种类型
                switch (javaType) {
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                        // 处理unsigned类型
                        Number number = (Number) value;
                        if (fieldMeta != null && fieldMeta.isUnsigned() && number.longValue() < 0) {
                            switch (buffer.getLength()) {
                                case 1: /* MYSQL_TYPE_TINY */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(TINYINT_MAX_VALUE
                                                                                          + number.intValue())));
                                    javaType = Types.SMALLINT; // 往上加一个量级
                                    break;

                                case 2: /* MYSQL_TYPE_SHORT */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(SMALLINT_MAX_VALUE
                                                                                          + number.intValue())));
                                    javaType = Types.INTEGER; // 往上加一个量级
                                    break;

                                case 3: /* MYSQL_TYPE_INT24 */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(MEDIUMINT_MAX_VALUE
                                                                                          + number.intValue())));
                                    javaType = Types.INTEGER; // 往上加一个量级
                                    break;

                                case 4: /* MYSQL_TYPE_LONG */
                                    columnBuilder.setValue(String.valueOf(Long.valueOf(INTEGER_MAX_VALUE
                                                                                       + number.longValue())));
                                    javaType = Types.BIGINT; // 往上加一个量级
                                    break;

                                case 8: /* MYSQL_TYPE_LONGLONG */
                                    columnBuilder.setValue(BIGINT_MAX_VALUE.add(BigInteger.valueOf(number.longValue()))
                                        .toString());
                                    javaType = Types.DECIMAL; // 往上加一个量级，避免执行出错
                                    break;
                            }
                        } else {
                            // 对象为number类型，直接valueof即可
                            columnBuilder.setValue(String.valueOf(value));
                        }
                        break;
                    case Types.REAL: // float
                    case Types.DOUBLE: // double
                        // 对象为number类型，直接valueof即可
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.BIT:// bit
                        // 对象为number类型
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.DECIMAL:
                        columnBuilder.setValue(((BigDecimal) value).toPlainString());
                        break;
                    case Types.TIMESTAMP:
                        // 修复时间边界值
                        // String v = value.toString();
                        // v = v.substring(0, v.length() - 2);
                        // columnBuilder.setValue(v);
                        // break;
                    case Types.TIME:
                    case Types.DATE:
                        // 需要处理year
                        columnBuilder.setValue(value.toString());
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        // fixed text encoding
                        // https://github.com/AlibabaTech/canal/issues/18
                        // mysql binlog中blob/text都处理为blob类型，需要反查table
                        // meta，按编码解析text
                        if (fieldMeta != null && isText(fieldMeta.getColumnType())) {
                            columnBuilder.setValue(new String((byte[]) value, charset));
                            javaType = Types.CLOB;
                        } else {
                            // byte数组，直接使用iso-8859-1保留对应编码，浪费内存
                            columnBuilder.setValue(new String((byte[]) value, ISO_8859_1));
                            javaType = Types.BLOB;
                        }
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        columnBuilder.setValue(value.toString());
                        break;
                    default:
                        columnBuilder.setValue(value.toString());
                }

            }

            columnBuilder.setSqlType(javaType);
            // 设置是否update的标记位
            columnBuilder.setUpdated(isAfter
                                     && isUpdate(rowDataBuilder.getBeforeColumnsList(),
                                         columnBuilder.getIsNull() ? null : columnBuilder.getValue(),
                                         i));
            if (isAfter) {
                rowDataBuilder.addAfterColumns(columnBuilder.build());
            } else {
                rowDataBuilder.addBeforeColumns(columnBuilder.build());
            }
        }

        return tableError;

    }

    private Entry buildQueryEntry(String queryString, LogHeader logHeader, String tableName) {
        Header header = createHeader(binlogFileName, logHeader, "", tableName, EventType.QUERY);
        RowChange.Builder rowChangeBuider = RowChange.newBuilder();
        rowChangeBuider.setSql(queryString);
        rowChangeBuider.setEventType(EventType.QUERY);
        return createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
    }

    private Entry buildQueryEntry(String queryString, LogHeader logHeader) {
        Header header = createHeader(binlogFileName, logHeader, "", "", EventType.QUERY);
        RowChange.Builder rowChangeBuider = RowChange.newBuilder();
        rowChangeBuider.setSql(queryString);
        rowChangeBuider.setEventType(EventType.QUERY);
        return createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
    }

    private Header createHeader(String binlogFile, LogHeader logHeader, String schemaName, String tableName,
                                EventType eventType) {
        // header会做信息冗余,方便以后做检索或者过滤
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setVersion(version);
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(logHeader.getLogPos() - logHeader.getEventLen());
        headerBuilder.setServerId(logHeader.getServerId());
        headerBuilder.setServerenCode(UTF_8);// 经过java输出后所有的编码为unicode
        headerBuilder.setExecuteTime(logHeader.getWhen() * 1000L);
        headerBuilder.setSourceType(Type.MYSQL);
        if (eventType != null) {
            headerBuilder.setEventType(eventType);
        }
        if (schemaName != null) {
            headerBuilder.setSchemaName(schemaName);
        }
        if (tableName != null) {
            headerBuilder.setTableName(tableName);
        }
        headerBuilder.setEventLength(logHeader.getEventLen());
        return headerBuilder.build();
    }

    private boolean isUpdate(List<Column> bfColumns, String newValue, int index) {
        if (bfColumns == null) {
            throw new CanalParseException("ERROR ## the bfColumns is null");
        }

        if (index < 0) {
            return false;
        }

        for (Column column : bfColumns) {
            if (column.getIndex() == index) {// 比较before / after的column index
                if (column.getIsNull() && newValue == null) {
                    // 如果全是null
                    return false;
                } else if (newValue != null && (!column.getIsNull() && column.getValue().equals(newValue))) {
                    // fixed issue #135, old column is Null
                    // 如果不为null，并且相等
                    return false;
                }
            }
        }

        // 比如nolob/minial模式下,可能找不到before记录,认为是有变化
        return true;
    }

    private TableMeta getTableMeta(String dbName, String tbName, boolean useCache, EntryPosition position) {
        try {
            return tableMetaCache.getTableMeta(dbName, tbName, useCache, position);
        } catch (Exception e) {
            String message = ExceptionUtils.getRootCauseMessage(e);
            if (filterTableError) {
                if (StringUtils.contains(message, "errorNumber=1146") && StringUtils.contains(message, "doesn't exist")) {
                    return null;
                }
            }

            throw new CanalParseException(e);
        }
    }

    private boolean isText(String columnType) {
        return "LONGTEXT".equalsIgnoreCase(columnType) || "MEDIUMTEXT".equalsIgnoreCase(columnType)
               || "TEXT".equalsIgnoreCase(columnType) || "TINYTEXT".equalsIgnoreCase(columnType);
    }
    
    private boolean isAliSQLHeartBeat(String schema, String table) {
        return "test".equalsIgnoreCase(schema) && "heartbeat".equalsIgnoreCase(table);
    }

    private boolean isRDSHeartBeat(String schema, String table) {
        return "mysql".equalsIgnoreCase(schema) && "ha_health_check".equalsIgnoreCase(table);
    }

    public static TransactionBegin createTransactionBegin(long threadId) {
        TransactionBegin.Builder beginBuilder = TransactionBegin.newBuilder();
        beginBuilder.setThreadId(threadId);
        return beginBuilder.build();
    }

    public static TransactionEnd createTransactionEnd(long transactionId) {
        TransactionEnd.Builder endBuilder = TransactionEnd.newBuilder();
        endBuilder.setTransactionId(String.valueOf(transactionId));
        return endBuilder.build();
    }

    public static Pair createSpecialPair(String key, String value) {
        Pair.Builder pairBuilder = Pair.newBuilder();
        pairBuilder.setKey(key);
        pairBuilder.setValue(value);
        return pairBuilder.build();
    }

    public static Entry createEntry(Header header, EntryType entryType, ByteString storeValue) {
        Entry.Builder entryBuilder = Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        entryBuilder.setStoreValue(storeValue);
        return entryBuilder.build();
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public void setNameFilter(AviaterRegexFilter nameFilter) {
        this.nameFilter = nameFilter;
    }

    public void setNameBlackFilter(AviaterRegexFilter nameBlackFilter) {
        this.nameBlackFilter = nameBlackFilter;
    }

    public void setTableMetaCache(TableMetaCache tableMetaCache) {
        this.tableMetaCache = tableMetaCache;
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

    public void setFilterTableError(boolean filterTableError) {
        this.filterTableError = filterTableError;
    }

    public void setFilterRows(boolean filterRows) {
        this.filterRows = filterRows;
    }

}

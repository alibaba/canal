package com.alibaba.otter.canal.parse.inbound.oceanbase.logproxy;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;

/**
 * 基于LogProxy的BinlogParser实现
 *
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public class LogProxyMessageParser extends AbstractBinlogParser<LogMessage> {

    public static final Logger logger = LoggerFactory.getLogger(LogProxyMessageParser.class);

    public static final String LOGFILE_NAME = "LogProxyMessageFile";

    private static final Map<DataMessage.Record.Field.Type, String> MYSQL_TYPE = new HashMap<DataMessage.Record.Field.Type, String>() {{
        put(DataMessage.Record.Field.Type.INT8, "tinyint");
        put(DataMessage.Record.Field.Type.INT16, "smallint");
        put(DataMessage.Record.Field.Type.INT24, "mediumint");
        put(DataMessage.Record.Field.Type.INT32, "int");
        put(DataMessage.Record.Field.Type.INT64, "bigint");
        put(DataMessage.Record.Field.Type.DECIMAL, "decimal");
        put(DataMessage.Record.Field.Type.FLOAT, "float");
        put(DataMessage.Record.Field.Type.DOUBLE, "double");
        put(DataMessage.Record.Field.Type.NULL, "null");
        put(DataMessage.Record.Field.Type.TIMESTAMP, "timestamp");
        put(DataMessage.Record.Field.Type.DATE, "date");
        put(DataMessage.Record.Field.Type.TIME, "time");
        put(DataMessage.Record.Field.Type.DATETIME, "datetime");
        put(DataMessage.Record.Field.Type.YEAR, "year");
        put(DataMessage.Record.Field.Type.BIT, "bit");
        put(DataMessage.Record.Field.Type.ENUM, "enum");
        put(DataMessage.Record.Field.Type.SET, "set");
        put(DataMessage.Record.Field.Type.BLOB, "blob");
        put(DataMessage.Record.Field.Type.GEOMETRY, "geometry");
        put(DataMessage.Record.Field.Type.JSON, "json");
        put(DataMessage.Record.Field.Type.BINARY, "binary");
        put(DataMessage.Record.Field.Type.TIMESTAMP_WITH_TIME_ZONE, "timestamp");
        put(DataMessage.Record.Field.Type.TIMESTAMP_WITH_LOCAL_TIME_ZONE, "timestamp");
        put(DataMessage.Record.Field.Type.TIMESTAMP_NANO, "timestamp");
        put(DataMessage.Record.Field.Type.STRING, "varchar");
    }};

    private static final Map<DataMessage.Record.Field.Type, Integer> JAVA_TYPE = new HashMap<DataMessage.Record.Field.Type, Integer>() {{
        put(DataMessage.Record.Field.Type.INT8, Types.TINYINT);
        put(DataMessage.Record.Field.Type.INT16, Types.SMALLINT);
        put(DataMessage.Record.Field.Type.INT24, Types.INTEGER);
        put(DataMessage.Record.Field.Type.INT32, Types.INTEGER);
        put(DataMessage.Record.Field.Type.INT64, Types.BIGINT);
        put(DataMessage.Record.Field.Type.DECIMAL, Types.DECIMAL);
        put(DataMessage.Record.Field.Type.FLOAT, Types.REAL);
        put(DataMessage.Record.Field.Type.DOUBLE, Types.DOUBLE);
        put(DataMessage.Record.Field.Type.NULL, Types.NULL);
        put(DataMessage.Record.Field.Type.TIMESTAMP, Types.TIMESTAMP);
        put(DataMessage.Record.Field.Type.DATE, Types.DATE);
        put(DataMessage.Record.Field.Type.TIME, Types.TIME);
        put(DataMessage.Record.Field.Type.DATETIME, Types.TIMESTAMP);
        put(DataMessage.Record.Field.Type.YEAR, Types.VARCHAR);
        put(DataMessage.Record.Field.Type.BIT, Types.BIT);
        put(DataMessage.Record.Field.Type.ENUM, Types.CHAR);
        put(DataMessage.Record.Field.Type.SET, Types.CHAR);
        put(DataMessage.Record.Field.Type.BLOB, Types.BLOB);
        put(DataMessage.Record.Field.Type.GEOMETRY, Types.VARCHAR);
        put(DataMessage.Record.Field.Type.JSON, Types.VARCHAR);
        put(DataMessage.Record.Field.Type.BINARY, Types.BINARY);
        put(DataMessage.Record.Field.Type.TIMESTAMP_WITH_TIME_ZONE, Types.TIMESTAMP);
        put(DataMessage.Record.Field.Type.TIMESTAMP_WITH_LOCAL_TIME_ZONE, Types.TIMESTAMP);
        put(DataMessage.Record.Field.Type.TIMESTAMP_NANO, Types.TIMESTAMP);
        put(DataMessage.Record.Field.Type.STRING, Types.VARCHAR);
    }};

    private boolean    filterDmlInsert = false;
    private boolean    filterDmlUpdate = false;
    private boolean    filterDmlDelete = false;
    private String     tenant;
    private boolean    excludeTenantInDbName;
    private AtomicLong receivedMessageCount;

    @Override
    public CanalEntry.Entry parse(LogMessage message, boolean isSeek) throws CanalParseException {
        // LogProxy的连接参数中包含DML的白名单，因此这里只需要检查黑名单
        String name = getTargetDbName(message.getDbName()) + "." + message.getTableName();
        if (nameBlackFilter != null && nameBlackFilter.filter(name)) {
            return null;
        }
        CanalEntry.Entry entry = null;
        try {
            switch (message.getOpt()) {
                case INSERT:
                    entry = parseDmlRecord(message, CanalEntry.EventType.INSERT);
                    break;
                case UPDATE:
                    entry = parseDmlRecord(message, CanalEntry.EventType.UPDATE);
                    break;
                case DELETE:
                    entry = parseDmlRecord(message, CanalEntry.EventType.DELETE);
                    break;
                case BEGIN:
                    entry = parseBeginRecord(message);
                    break;
                case COMMIT:
                    entry = parseCommitRecord(message);
                    break;
                case DDL:
                    entry = parseDdlRecord(message);
                    break;
                case HEARTBEAT:
                    entry = parseHeartBeatRecord(message);
                    break;
                case DML:
                case INDEX_REPLACE:
                case INDEX_INSERT:
                case INDEX_UPDATE:
                case INDEX_DELETE:
                case REPLACE:
                case ROLLBACK:
                case CONSISTENCY_TEST:
                case UNKNOWN:
                    break;
                default:
                    throw new IllegalStateException("Unexpected record type: " + message.getOpt());
            }
        } catch (Throwable e) {
            logger.error("parse error: ", e);
            throw new CanalParseException(e);
        }
        return entry;
    }

    @Override
    public void reset() {

    }

    private CanalEntry.Entry parseDmlRecord(LogMessage message, CanalEntry.EventType eventType) {
        messageCount();
        if (filterQueryDml || (filterDmlInsert && eventType.equals(CanalEntry.EventType.INSERT)) || (filterDmlUpdate
                                                                                                     && eventType.equals(
            CanalEntry.EventType.UPDATE)) || (filterDmlDelete && eventType.equals(CanalEntry.EventType.DELETE))) {
            return null;
        }
        CanalEntry.Header header = createHeader(message, eventType, 1);
        CanalEntry.RowData rowData = dmlRowData(message);
        CanalEntry.RowChange.Builder builder = CanalEntry.RowChange.newBuilder();
        builder.setIsDdl(false);
        builder.addRowDatas(rowData);
        builder.setEventType(eventType);
        return createEntry(header, CanalEntry.EntryType.ROWDATA, builder.build().toByteString());
    }

    private CanalEntry.Entry parseBeginRecord(LogMessage message) throws Exception {
        messageCount();
        CanalEntry.TransactionBegin transactionBegin = createTransactionBegin(Long.parseLong(message.getThreadId()));
        CanalEntry.Header header = createHeader(message, CanalEntry.EventType.QUERY, 0);
        return createEntry(header, CanalEntry.EntryType.TRANSACTIONBEGIN, transactionBegin.toByteString());
    }

    private CanalEntry.Entry parseCommitRecord(LogMessage message) {
        messageCount();
        CanalEntry.TransactionEnd transactionEnd = createTransactionEnd(0);
        CanalEntry.Header header = createHeader(message, CanalEntry.EventType.XACOMMIT, 0);
        return createEntry(header, CanalEntry.EntryType.TRANSACTIONEND, transactionEnd.toByteString());
    }

    private CanalEntry.Entry parseDdlRecord(LogMessage message) {
        messageCount();
        if (filterQueryDdl) {
            return null;
        }
        CanalEntry.EventType eventType = CanalEntry.EventType.QUERY;
        String ddl = message.getFieldList().get(0).getValue().toString(charset.name());

        // DDL类型的LogMessage没有存表名，并且SQL本身可能缺少库名前缀。
        // 因此这里从SQL中提取表名填补到CanalEntry，并补全SQL中的表名为`db`.`table`格式
        String dbName = null;
        String table = null;
        List<DdlResult> ddlResults = DruidDdlParser.parse(ddl, getTargetDbName(message.getDbName()));
        if (ddlResults.size() > 0) {
            DdlResult ddlResult = ddlResults.get(0);
            eventType = ddlResult.getType();
            dbName = ddlResult.getSchemaName();
            table = ddlResult.getTableName();

            // LogProxy对DDL没有过滤，而且ddl的message中没有表名，这里单独处理
            String tableFullName = dbName + "." + table;
            if (nameFilter != null && !nameFilter.filter(tableFullName)) {
                return null;
            }
            if (nameBlackFilter != null && nameBlackFilter.filter(tableFullName)) {
                return null;
            }

            if (eventType == CanalEntry.EventType.ALTER) {
                ddl = completeTableNameInSql(ddl, dbName, table);
            }
        }

        CanalEntry.Header header = createHeader(message, eventType, 0, table);
        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setIsDdl(true);
        rowChangeBuilder.setSql(ddl);
        rowChangeBuilder.setEventType(eventType);
        if (dbName != null) {
            rowChangeBuilder.setDdlSchemaName(dbName);
        }
        return createEntry(header, CanalEntry.EntryType.ROWDATA, rowChangeBuilder.build().toByteString());
    }

    private CanalEntry.Entry parseHeartBeatRecord(LogMessage message) {
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setExecuteTime(Long.parseLong(message.getTimestamp())*1000);
        headerBuilder.setEventType(CanalEntry.EventType.MHEARTBEAT);
        CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        entryBuilder.setEntryType(CanalEntry.EntryType.HEARTBEAT);
        return entryBuilder.build();
    }

    private CanalEntry.Header createHeader(LogMessage message, CanalEntry.EventType eventType, int rowsCount) {
        return createHeader(message, eventType, rowsCount, null);
    }

    private CanalEntry.Header createHeader(LogMessage message, CanalEntry.EventType eventType, int rowsCount, String ddlTableName) {
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setServerenCode(StandardCharsets.UTF_8.toString());
        headerBuilder.setLogfileName(LOGFILE_NAME);
        headerBuilder.setVersion(message.getVersion());
        headerBuilder.setLogfileOffset(message.getFileOffset());
        headerBuilder.setEventType(eventType);
        headerBuilder.setExecuteTime(Long.parseLong(message.getTimestamp())*1000);

        if (message.getOB10UniqueId() != null) {
            headerBuilder.setGtid(message.getOB10UniqueId());
        }
        if (StringUtils.isNotBlank(message.getServerId())) {
            headerBuilder.setServerId(Long.parseLong(message.getServerId()));
        }
        String dbName = getTargetDbName(message.getDbName());
        if (StringUtils.isNotBlank(dbName)) {
            headerBuilder.setSchemaName(dbName);
        }
        String tableName = message.getTableName();
        if (StringUtils.isEmpty(tableName) && ddlTableName != null) {
            tableName = ddlTableName;
        }
        if (StringUtils.isNotBlank(tableName)) {
            headerBuilder.setTableName(tableName);
        }
        CanalEntry.Pair pair = createSpecialPair("rowsCount", String.valueOf(rowsCount));
        headerBuilder.addProps(pair);
        return headerBuilder.build();
    }

    private CanalEntry.RowData dmlRowData(LogMessage message) {
        String name = message.getDbName() + "." + message.getTableName();
        List<String> fieldWhiteList = fieldFilterMap.get(name.toUpperCase());
        List<String> fieldBlackList = fieldBlackFilterMap.get(name.toUpperCase());

        Set<String> uks = null;
        if (message.getUniqueColNames() != null) {
            String[] uniqueColNames = StringUtils.split(message.getUniqueColNames(), ",");
            if (uniqueColNames != null && uniqueColNames.length > 0) {
                uks = new HashSet<>(Arrays.asList(uniqueColNames));
            }
        }

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        List<DataMessage.Record.Field> fields = message.getFieldList();
        int index = 0;
        for (int i = 0; i < fields.size(); i++) {
            DataMessage.Record.Field field = fields.get(i);

            if (!needField(fieldWhiteList, fieldBlackList, field.getFieldname())) {
                continue;
            }

            CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder();
            columnBuilder.setName(field.getFieldname());
            columnBuilder.setIndex(index);
            columnBuilder.setIsKey(field.primaryKey || (uks != null && uks.contains(field.getFieldname())));
            if (field.getValue() == null) {
                columnBuilder.setIsNull(true);
            } else {
                columnBuilder.setMysqlType(MYSQL_TYPE.getOrDefault(field.getType(), ""));
                columnBuilder.setSqlType(JAVA_TYPE.getOrDefault(field.getType(), 0));
                columnBuilder.setValue(field.getValue().toString(charset.name()));
            }

            switch (message.getOpt()) {
                case INSERT:
                    rowDataBuilder.addAfterColumns(columnBuilder.build());
                    index++;
                    break;
                case UPDATE:
                    if (i % 2 == 0) {
                        columnBuilder.setUpdated(false);
                        rowDataBuilder.addBeforeColumns(columnBuilder.build());
                    } else {
                        columnBuilder.setUpdated(isUpdate(rowDataBuilder.getBeforeColumnsList(),
                            columnBuilder.getIsNull() ? null : columnBuilder.getValue(),
                            index++));
                        rowDataBuilder.addAfterColumns(columnBuilder.build());
                    }
                    break;
                case DELETE:
                    rowDataBuilder.addBeforeColumns(columnBuilder.build());
                    index++;
                    break;
                default:
                    throw new IllegalStateException("Unexpected type for row data: " + message.getOpt());
            }
        }
        return rowDataBuilder.build();
    }

    /**
     * 获取目标库的名称
     *
     * @param dbName 原始库名
     * @return 目标库名
     */
    private String getTargetDbName(String dbName) {
        if (excludeTenantInDbName && StringUtils.isNotBlank(dbName) && dbName.length() > tenant.length()) {
            return dbName.replace(tenant + ".", "");
        }
        return dbName;
    }

    private void messageCount() {
        receivedMessageCount.addAndGet(1);
    }

    public void setFilterDmlInsert(boolean filterDmlInsert) {
        this.filterDmlInsert = filterDmlInsert;
    }

    public void setFilterDmlUpdate(boolean filterDmlUpdate) {
        this.filterDmlUpdate = filterDmlUpdate;
    }

    public void setFilterDmlDelete(boolean filterDmlDelete) {
        this.filterDmlDelete = filterDmlDelete;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public void setExcludeTenantInDbName(boolean excludeTenantInDbName) {
        this.excludeTenantInDbName = excludeTenantInDbName;
    }

    public void setReceivedMessageCount(AtomicLong receivedMessageCount) {
        this.receivedMessageCount = receivedMessageCount;
    }
}

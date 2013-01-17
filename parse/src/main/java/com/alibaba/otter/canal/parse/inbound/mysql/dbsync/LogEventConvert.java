package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.BitSet;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
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
import com.google.protobuf.ByteString;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.DeleteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RotateLogEvent;
import com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer;
import com.taobao.tddl.dbsync.binlog.event.RowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent;
import com.taobao.tddl.dbsync.binlog.event.UpdateRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.WriteRowsLogEvent;
import com.taobao.tddl.dbsync.binlog.event.XidLogEvent;
import com.taobao.tddl.dbsync.binlog.event.TableMapLogEvent.ColumnInfo;

/**
 * 基于{@linkplain LogEvent}转化为Entry对象的处理
 * 
 * @author jianghang 2013-1-17 下午02:41:14
 * @version 1.0.0
 */
public class LogEventConvert extends AbstractCanalLifeCycle implements BinlogParser<LogEvent> {

    private static final String ISO_8859_1     = "ISO-8859-1";
    private static final String UTF_8          = "UTF-8";
    private int                 version        = 1;
    private static final String BEGIN          = "BEGIN";
    private static final String COMMIT         = "COMMIT";
    private String              binlogFileName = "mysql-bin.000001";
    private Charset             charset        = Charset.defaultCharset();

    public Entry parse(LogEvent logEvent) throws CanalParseException {
        int eventType = logEvent.getHeader().getType();
        switch (eventType) {
            case LogEvent.ROTATE_EVENT:
                binlogFileName = ((RotateLogEvent) logEvent).getFilename();
                break;
            case LogEvent.QUERY_EVENT:
                return parserQueryEvent((QueryLogEvent) logEvent);
            case LogEvent.XID_EVENT:
                return paserXidEvent((XidLogEvent) logEvent);
            case LogEvent.TABLE_MAP_EVENT:
                break;
            case LogEvent.WRITE_ROWS_EVENT:
                return parseRowsEvent((WriteRowsLogEvent) logEvent);
            case LogEvent.UPDATE_ROWS_EVENT:
                return parseRowsEvent((UpdateRowsLogEvent) logEvent);
            case LogEvent.DELETE_ROWS_EVENT:
                return parseRowsEvent((DeleteRowsLogEvent) logEvent);
            default:
                break;
        }

        return null;
    }

    public void reset() {
        // do nothing
    }

    private Entry parserQueryEvent(QueryLogEvent event) {
        String queryString = event.getQuery();
        if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            TransactionBegin transactionBegin = createTransactionBegin(event.getExecTime());
            Header header = createHeader(binlogFileName, event.getHeader(), "", "");
            return createEntry(header, EntryType.TRANSACTIONBEGIN, transactionBegin.toByteString());
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            TransactionEnd transactionEnd = createTransactionEnd(0L, event.getWhen()); // MyISAM可能不会有xid事件
            Header header = createHeader(binlogFileName, event.getHeader(), "", "");
            return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());
        } else {
            // TODO : DDL语句处理
        }

        return null;
    }

    private Entry paserXidEvent(XidLogEvent event) {
        TransactionEnd transactionEnd = createTransactionEnd(event.getXid(), event.getWhen());
        Header header = createHeader(binlogFileName, event.getHeader(), "", "");
        return createEntry(header, EntryType.TRANSACTIONEND, transactionEnd.toByteString());
    }

    private Entry parseRowsEvent(RowsLogEvent event) {
        try {
            Header header = createHeader(binlogFileName, event.getHeader(), "", "");
            RowChange.Builder rowChangeBuider = RowChange.newBuilder();
            rowChangeBuider.setTableId(event.getTableId());
            EventType eventType = null;
            if (LogEvent.WRITE_ROWS_EVENT == event.getHeader().getType()) {
                eventType = EventType.INSERT;
            } else if (LogEvent.UPDATE_ROWS_EVENT == event.getHeader().getType()) {
                eventType = EventType.UPDATE;
            } else if (LogEvent.DELETE_ROWS_EVENT == event.getHeader().getType()) {
                eventType = EventType.DELETE;
            } else {
                throw new CanalParseException("unsupport event type :" + event.getHeader().getType());
            }
            rowChangeBuider.setEventType(eventType);

            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();

            while (buffer.nextOneRow(columns)) {
                // 处理row记录
                RowData.Builder rowDataBuilder = RowData.newBuilder();
                if (EventType.INSERT == eventType) {
                    // insert的记录放在before字段中
                    parseOneRow(rowDataBuilder, event, buffer, columns, true);
                } else if (EventType.DELETE == eventType) {
                    // delete的记录放在before字段中
                    parseOneRow(rowDataBuilder, event, buffer, columns, false);
                } else {
                    // update需要处理before/after
                    parseOneRow(rowDataBuilder, event, buffer, columns, false);
                    parseOneRow(rowDataBuilder, event, buffer, event.getChangeColumns(), true);
                }

                rowChangeBuider.addRowDatas(rowDataBuilder.build());
            }
            return createEntry(header, EntryType.ROWDATA, rowChangeBuider.build().toByteString());
        } catch (Exception e) {
            throw new CanalParseException("parse row data failed.", e);
        }
    }

    private void parseOneRow(RowData.Builder rowDataBuilder, RowsLogEvent event, RowsLogBuffer buffer, BitSet cols,
                             boolean isAfter) throws UnsupportedEncodingException {
        TableMapLogEvent map = event.getTable();
        if (map == null) {
            throw new CanalParseException("not found TableMap with tid=" + event.getTableId());
        }

        final int columnCnt = map.getColumnCnt();
        final ColumnInfo[] columnInfo = map.getColumnInfo();
        for (int i = 0; i < columnCnt; i++) {
            ColumnInfo info = columnInfo[i];
            buffer.nextValue(info.type, info.meta);

            Column.Builder columnBuilder = Column.newBuilder();
            columnBuilder.setIndex(i);
            columnBuilder.setIsNull(false);
            columnBuilder.setUpdated(cols.get(i) && isAfter);
            if (buffer.isNull()) {
                columnBuilder.setIsNull(true);
            } else {
                final int javaType = buffer.getJavaType();
                final Serializable value = buffer.getValue();
                // 处理各种类型
                switch (javaType) {
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                    case Types.REAL: // float
                    case Types.DOUBLE: // double
                    case Types.BIT:// bit
                        // 对象为number类型，直接valueof即可
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.DECIMAL:
                        columnBuilder.setValue(((BigDecimal) value).toPlainString());
                        break;
                    case Types.TIMESTAMP:
                        String v = value.toString();
                        v = v.substring(0, v.length() - 2);
                        columnBuilder.setValue(v);
                        break;
                    case Types.TIME:
                    case Types.DATE:
                        // 需要处理year
                        columnBuilder.setValue(value.toString());
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        // byte数组，直接使用iso-8859-1保留对应编码，浪费内存
                        columnBuilder.setValue(new String((byte[]) value, ISO_8859_1));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        // 本身对象为string
                        columnBuilder.setValue(value.toString());
                        break;
                    default:
                        columnBuilder.setValue(value.toString());
                }

                if (isAfter) {
                    rowDataBuilder.addAfterColumns(columnBuilder.build());
                } else {
                    rowDataBuilder.addBeforeColumns(columnBuilder.build());
                }
            }
        }

    }

    private Header createHeader(String binlogFile, LogHeader logHeader, String schemaName, String tableName) {
        // header会做信息冗余,方便以后做检索或者过滤
        Header.Builder headerBuilder = Header.newBuilder();
        headerBuilder.setVersion(version);
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(logHeader.getLogPos() - logHeader.getEventLen());
        headerBuilder.setServerId(logHeader.getServerId());
        headerBuilder.setServerenCode(UTF_8);// 经过java输出后所有的编码为unicode
        headerBuilder.setExecuteTime(logHeader.getWhen());
        headerBuilder.setSourceType(Type.MYSQL);
        headerBuilder.setSchemaName(StringUtils.lowerCase(schemaName));
        headerBuilder.setTableName(StringUtils.lowerCase(tableName));
        headerBuilder.setEventLength(logHeader.getEventLen());
        return headerBuilder.build();
    }

    public static TransactionBegin createTransactionBegin(long executeTime) {
        TransactionBegin.Builder beginBuilder = TransactionBegin.newBuilder();
        beginBuilder.setExecuteTime(executeTime);
        return beginBuilder.build();
    }

    public static TransactionEnd createTransactionEnd(long transactionId, long executeTime) {
        TransactionEnd.Builder endBuilder = TransactionEnd.newBuilder();
        endBuilder.setTransactionId(String.valueOf(transactionId));
        endBuilder.setExecuteTime(executeTime);
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

}

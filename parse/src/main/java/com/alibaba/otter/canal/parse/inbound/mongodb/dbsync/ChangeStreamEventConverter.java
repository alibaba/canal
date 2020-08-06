package com.alibaba.otter.canal.parse.inbound.mongodb.dbsync;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import jdk.nashorn.internal.runtime.Undefined;
import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 基于{@linkplain ChangeStreamEvent}转化为Entry对象的处理
 *
 * @author jiabao.sun 2020-07-13 15:23:30
 * @version 1.0.0
 */
public class ChangeStreamEventConverter extends AbstractCanalLifeCycle implements BinlogParser<ChangeStreamEvent> {

    private static Logger               logger                     = LoggerFactory.getLogger(ChangeStreamEventConverter.class);

    private static final String         ID_FIELD_NAME              = "_id";
    private static final String         CLASS_FIELD_NAME           = "_class";

    public  static final String         UTF_8                      = "UTF-8";
    public  static final String         ISO_8859_1                 = "ISO-8859-1";

    private volatile AviaterRegexFilter nameFilter;
    private volatile AviaterRegexFilter nameBlackFilter;

    private Map<String, List<String>>   fieldFilterMap             = new HashMap<String, List<String>>();
    private Map<String, List<String>>   fieldBlackFilterMap        = new HashMap<String, List<String>>();

    @Override
    public CanalEntry.Entry parse(ChangeStreamEvent logEvent, boolean isSeek) throws CanalParseException {
        if (logEvent == null) {
            logger.error("ignored null logEvent , must check!");
            return null;
        }

        if (logEvent.getNamespace() == null) {
            logger.error("ignored logEvent of {}, mongo namespace is not present", logEvent);
            return null;
        }

        String fullName = logEvent.getNamespace().getFullName();

        boolean ignore = true;
        //未命中黑名单，并且命中白名单
        if (nameBlackFilter == null || !nameBlackFilter.filter(fullName)) {
            if (nameFilter == null || nameFilter.filter(fullName)) {
                ignore = false;
            }
        }

        if (ignore) {
            logger.debug("ignored log event of {}", fullName);
            return null;
        }

        try {
            switch (logEvent.getOperationType()) {
                case INSERT:
                    return parseInsert(logEvent);
                case DELETE:
                    return parseDelete(logEvent);
                case UPDATE:
                    return parseUpdate(logEvent);
                case REPLACE:
                    return parseReplace(logEvent);
                case INVALIDATE:
                    logger.warn("invalidate event received, watch channel will be closed. {}", logEvent);
                    return null;
                case DROP:
                case DROP_DATABASE:
                case RENAME:
                case OTHER:
                default:
                    logger.debug("ignored opType of {}, {}", logEvent.getOperationType(), logEvent);
                    return null;
            }
        } catch (Exception e) {
            throw new CanalParseException("parse mongo row data failed.", e);
        }
    }

    @Override
    public void reset() {
        //do nothing
    }

    private CanalEntry.Header.Builder commonHeader(ChangeStreamEvent logEvent) {
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setSourceType(CanalEntry.Type.MONGODB);
        headerBuilder.setSchemaName(logEvent.getNamespace().getDatabaseName());
        headerBuilder.setTableName(logEvent.getNamespace().getCollectionName());
        headerBuilder.setLogfileName(ChangeStreamEvent.LOG_FILE_COLLECTION);
        headerBuilder.setServerenCode(UTF_8);// 经过java输出后所有的编码为unicode
        //BsonTimestamp转毫秒时间戳
        headerBuilder.setExecuteTime(BsonConverter.convertTime(logEvent.getClusterTime(), TimeUnit.MILLISECONDS));
        //offset记录完整BsonTimestamp: second(32) inc(32)
        headerBuilder.setLogfileOffset(logEvent.getClusterTime().getValue());

        return headerBuilder;
    }

    private CanalEntry.Entry parseInsert(ChangeStreamEvent logEvent) {
        if (logEvent.getFullDocument() == null) {
            logger.error("error insert event, the full document is not present!");
            return null;
        }

        CanalEntry.Header.Builder headerBuilder = commonHeader(logEvent);
        headerBuilder.setEventType(CanalEntry.EventType.INSERT);

        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setEventType(CanalEntry.EventType.INSERT);
        rowChangeBuilder.setIsDdl(false);

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        for (Map.Entry<String, Object> field : logEvent.getFullDocument().entrySet()) {
            // 忽略 spring data _class
            if (CLASS_FIELD_NAME.equals(field.getKey())) {
                continue;
            }

            if (needField(logEvent.getNamespace().getFullName(), field.getKey())) {
                // 变更后列
                rowDataBuilder.addAfterColumns(extractColumn(field.getKey(), field.getValue(), true));
            }
        }

        rowChangeBuilder.addRowDatas(rowDataBuilder);

        return createEntry(headerBuilder.build(), CanalEntry.EntryType.ROWDATA, rowChangeBuilder.build().toByteString());
    }

    private CanalEntry.Entry parseDelete(ChangeStreamEvent logEvent) {
        if (logEvent.getDocumentKey() == null || !logEvent.getDocumentKey().containsKey(ID_FIELD_NAME)) {
            logger.error("invalid delete event, the document key is not present!");
            return null;
        }

        Object idVal = logEvent.getDocumentKey().get(ID_FIELD_NAME);
        if (idVal == null) {
            logger.error("invalid delete event, has not id field");
            return null;
        }

        CanalEntry.Header.Builder headerBuilder = commonHeader(logEvent);
        headerBuilder.setEventType(CanalEntry.EventType.DELETE);

        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setEventType(CanalEntry.EventType.DELETE);
        rowChangeBuilder.setIsDdl(false);

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        rowDataBuilder.addBeforeColumns(extractColumn(ID_FIELD_NAME, idVal, true));

        rowChangeBuilder.addRowDatas(rowDataBuilder);

        return createEntry(headerBuilder.build(), CanalEntry.EntryType.ROWDATA, rowChangeBuilder.build().toByteString());
    }

    private CanalEntry.Entry parseUpdate(ChangeStreamEvent logEvent) {
        if (logEvent.getUpdateDescription() == null) {
            logger.error("invalid update event, the updated description is not present!");
            return null;
        }

        if (logEvent.getDocumentKey() == null || !logEvent.getDocumentKey().containsKey(ID_FIELD_NAME)) {
            logger.error("invalid delete event, the document key is not present!");
            return null;
        }

        Object idVal = logEvent.getDocumentKey().get(ID_FIELD_NAME);
        if (idVal == null) {
            logger.error("invalid delete event, has not id field");
            return null;
        }

        CanalEntry.Header.Builder headerBuilder = commonHeader(logEvent);
        headerBuilder.setEventType(CanalEntry.EventType.UPDATE);

        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setEventType(CanalEntry.EventType.UPDATE);
        rowChangeBuilder.setIsDdl(false);

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();

        // set id
        rowDataBuilder.addAfterColumns(extractColumn(ID_FIELD_NAME, idVal, true));

        // 处理 $set
        if (logEvent.getUpdateDescription().getUpdatedFields() != null) {
            for (Map.Entry<String, Object> field : logEvent.getUpdateDescription().getUpdatedFields().entrySet()) {
                // 忽略 spring data _class
                if (CLASS_FIELD_NAME.equals(field.getKey())) {
                    continue;
                }

                if (!ID_FIELD_NAME.equals(field.getKey())) {
                    rowDataBuilder.addBeforeColumns(extractColumn(field.getKey(), field.getValue(), false));
                }

                // 变更后列
                rowDataBuilder.addAfterColumns(extractColumn(field.getKey(), field.getValue(), true));
            }
        }

        // 处理 删除的field
        if (CollectionUtils.isEmpty(logEvent.getUpdateDescription().getRemovedFields())) {
            for (String fieldName : logEvent.getUpdateDescription().getRemovedFields()) {
                // 忽略 spring data _class
                if (CLASS_FIELD_NAME.equals(fieldName)) {
                    continue;
                }

                CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder();
                columnBuilder.setName(fieldName);
                columnBuilder.setIsNull(true);

                if (!ID_FIELD_NAME.equals(fieldName)) {
                    rowDataBuilder.addBeforeColumns(columnBuilder);
                }

                // 变更后列
                rowDataBuilder.addAfterColumns(columnBuilder);
            }
        }

        rowChangeBuilder.addRowDatas(rowDataBuilder);

        return createEntry(headerBuilder.build(), CanalEntry.EntryType.ROWDATA, rowChangeBuilder.build().toByteString());
    }

    private CanalEntry.Entry parseReplace(ChangeStreamEvent logEvent) {
        if (logEvent.getFullDocument() == null) {
            logger.error("invalid replace event, the replaced document is not present!");
            return null;
        }

        CanalEntry.Header.Builder headerBuilder = commonHeader(logEvent);
        headerBuilder.setEventType(CanalEntry.EventType.UPDATE);

        CanalEntry.RowChange.Builder rowChangeBuilder = CanalEntry.RowChange.newBuilder();
        rowChangeBuilder.setEventType(CanalEntry.EventType.UPDATE);
        rowChangeBuilder.setIsDdl(false);

        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        for (Map.Entry<String, Object> field : logEvent.getFullDocument().entrySet()) {
            // 忽略 spring data _class
            if (CLASS_FIELD_NAME.equals(field.getKey())) {
                continue;
            }

            if (needField(logEvent.getNamespace().getFullName(), field.getKey())) {
                // mongo oplog没有变更前信息，这里mock所有字段，值为空
                if (!ID_FIELD_NAME.equals(field.getKey())) {
                    rowDataBuilder.addBeforeColumns(extractColumn(field.getKey(), field.getValue(), false));
                }

                // 变更后列
                rowDataBuilder.addAfterColumns(extractColumn(field.getKey(), field.getValue(), true));
            }
        }

        rowChangeBuilder.addRowDatas(rowDataBuilder);

        return createEntry(headerBuilder.build(), CanalEntry.EntryType.ROWDATA, rowChangeBuilder.build().toByteString());
    }

    private CanalEntry.Entry createEntry(CanalEntry.Header header, CanalEntry.EntryType entryType, ByteString storeValue) {
        CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        entryBuilder.setStoreValue(storeValue);
        return entryBuilder.build();
    }

    //字段过滤
    private boolean needField(String namespace, String columnName) {
        //获取字段过滤条件
        List<String> fieldList = fieldFilterMap.get(namespace.toUpperCase());
        List<String> blackFieldList = fieldBlackFilterMap.get(namespace.toUpperCase());

        if (fieldList == null || fieldList.isEmpty()) {
            return blackFieldList == null || blackFieldList.isEmpty() || !blackFieldList.contains(columnName.toUpperCase());
        } else {
            return fieldList.contains(columnName.toUpperCase());
        }
    }

    private CanalEntry.Column.Builder extractColumn(String fieldName, Object fieldVal, boolean isAfter) {

        CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder();

        columnBuilder.setName(fieldName);
        if (ID_FIELD_NAME.equals(fieldName)) {
            columnBuilder.setIsKey(true);
        }

        if (isAfter) {
            columnBuilder.setUpdated(true);
        }

        if (fieldVal == null || !isAfter) { //mongo oplog无变更前字段信息，在此mock column变更前为空
            columnBuilder.setIsNull(true);
        } else {
            columnBuilder.setIsNull(false);

            if (fieldVal instanceof ObjectId) {
                columnBuilder.setValue(((ObjectId) fieldVal).toHexString());
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof UUID) {
                columnBuilder.setValue(fieldVal.toString());
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof String) {
                columnBuilder.setValue(fieldVal.toString());
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof Integer) {
                columnBuilder.setValue(String.valueOf(fieldVal));
                columnBuilder.setSqlType(Types.INTEGER);
            } else if (fieldVal instanceof Long) {
                columnBuilder.setValue(String.valueOf(fieldVal));
                columnBuilder.setSqlType(Types.BIGINT);
            } else if (fieldVal instanceof Float || fieldVal instanceof Double) {
                columnBuilder.setValue(String.valueOf(fieldVal));
                columnBuilder.setSqlType(Types.DECIMAL);
            } else if (fieldVal instanceof BigInteger) {
                columnBuilder.setValue(String.valueOf(fieldVal));
                columnBuilder.setSqlType(Types.BIGINT);
            } else if (fieldVal instanceof BigDecimal) {
                columnBuilder.setValue(((BigDecimal) fieldVal).toPlainString());
                columnBuilder.setSqlType(Types.DECIMAL);
            } else if (fieldVal instanceof Number) { //Integer, Float, Double, Decimal128
                columnBuilder.setValue(String.valueOf(fieldVal));
                columnBuilder.setSqlType(Types.NUMERIC);
            } else if (fieldVal instanceof Boolean) {
                columnBuilder.setValue(fieldVal.toString());
                columnBuilder.setSqlType(Types.BOOLEAN);
            } else if (fieldVal instanceof Date) {
                ZonedDateTime dateTime = ZonedDateTime.ofInstant(((Date) fieldVal).toInstant(), ZoneId.systemDefault());
                columnBuilder.setValue(dateTime.format(DateTimeFormatter.ISO_INSTANT));
                columnBuilder.setSqlType(Types.TIMESTAMP);
            } else if (fieldVal instanceof BsonTimestamp) {
                ZonedDateTime dateTime = ZonedDateTime.ofInstant(
                        Instant.ofEpochSecond(((BsonTimestamp) fieldVal).getTime()), ZoneId.systemDefault());
                columnBuilder.setValue(dateTime.format(DateTimeFormatter.ISO_INSTANT));
                columnBuilder.setSqlType(Types.TIMESTAMP);
            } else if (fieldVal instanceof BsonDbPointer) {
                BsonDbPointer dbPointer = (BsonDbPointer) fieldVal;
                if (dbPointer.getId() != null) {
                    columnBuilder.setValue(dbPointer.getId().toHexString());
                } else {
                    columnBuilder.setIsNull(true);
                }
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof Symbol) {
                Symbol symbol = (Symbol) fieldVal;
                if (symbol.getSymbol() != null) {
                    columnBuilder.setValue(symbol.getSymbol());
                } else {
                    columnBuilder.setIsNull(true);
                }
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof BsonRegularExpression) {
                BsonRegularExpression regularExpression = (BsonRegularExpression) fieldVal;
                if (!Strings.isNullOrEmpty(regularExpression.getPattern())) {
                    columnBuilder.setValue(regularExpression.getPattern());
                } else {
                    columnBuilder.setIsNull(true);
                }
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof Binary) {
                Binary binary = (Binary) fieldVal;
                if (binary.getData() != null) {
                    try {
                        columnBuilder.setValue(new String(binary.getData(), ISO_8859_1));
                    } catch (UnsupportedEncodingException e) {
                        throw new IllegalArgumentException("illegal encoding ", e);
                    }
                } else {
                    columnBuilder.setIsNull(true);
                }

                columnBuilder.setSqlType(Types.BLOB);
            } else if (fieldVal instanceof Iterable) {
                columnBuilder.setValue(JSON.toJSONString(fieldVal));
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof Map) { //Document is also Map
                columnBuilder.setValue(JSON.toJSONString(fieldVal));
                columnBuilder.setSqlType(Types.VARCHAR);
            } else if (fieldVal instanceof Undefined || fieldVal instanceof MaxKey
                    || fieldVal instanceof MinKey || fieldVal instanceof Code) {
                columnBuilder.setIsNull(true);
                logger.warn("ignored type of : {}", fieldVal.getClass().getName());
            } else {
                logger.error("unsupported type of {}", fieldVal);
                throw new IllegalStateException("unsupported type of {}" + fieldVal);
            }
        }

        return columnBuilder;
    }

    public void setFieldFilterMap(Map<String, List<String>> fieldFilterMap) {
        if (fieldFilterMap != null) {
            this.fieldFilterMap = fieldFilterMap;
        } else {
            this.fieldFilterMap = new HashMap<String, List<String>>();
        }


        for (Map.Entry<String, List<String>> entry : this.fieldFilterMap.entrySet()) {
            logger.warn("--> init field filter : " + entry.getKey() + "->" + entry.getValue());
        }
    }

    public void setFieldBlackFilterMap(Map<String, List<String>> fieldBlackFilterMap) {
        if (fieldBlackFilterMap != null) {
            this.fieldBlackFilterMap = fieldBlackFilterMap;
        } else {
            this.fieldBlackFilterMap = new HashMap<String, List<String>>();
        }

        for (Map.Entry<String, List<String>> entry : this.fieldBlackFilterMap.entrySet()) {
            logger.warn("--> init field black filter : " + entry.getKey() + "->" + entry.getValue());
        }
    }

    public void setNameFilter(AviaterRegexFilter nameFilter) {
        this.nameFilter = nameFilter;
    }

    public void setNameBlackFilter(AviaterRegexFilter nameBlackFilter) {
        this.nameBlackFilter = nameBlackFilter;
    }

}

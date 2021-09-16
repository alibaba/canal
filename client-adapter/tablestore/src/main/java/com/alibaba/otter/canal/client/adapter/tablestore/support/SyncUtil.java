package com.alibaba.otter.canal.client.adapter.tablestore.support;


import com.alibaba.otter.canal.client.adapter.tablestore.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.tablestore.enums.TablestoreFieldType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Map;

public class SyncUtil {
    private static final Logger logger  = LoggerFactory.getLogger(SyncUtil.class);


    public static PrimaryKeyValue getPrimaryKeyValue(Object value, TablestoreFieldType fieldType) {
        Object tablestoreValue;
        switch (fieldType) {
            case STRING:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return PrimaryKeyValue.fromString((String)tablestoreValue);
            case INT:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return PrimaryKeyValue.fromLong((long)tablestoreValue);
            case BINARY:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return PrimaryKeyValue.fromBinary((byte[])tablestoreValue);

            default:
                return PrimaryKeyValue.fromString(value.toString());

        }
    }

    private static Object getTablestoreValue(Object value, TablestoreFieldType type) {
        switch (type) {
            case STRING:
                if (value instanceof byte[]) {
                    return new String(((byte[])value));
                }
                return value.toString();
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else if (value instanceof Timestamp) {
                    return ((Timestamp) value).getTime();
                } else if (value instanceof String) {
                    try {
                        return Long.parseLong((String) value);
                    } catch (NumberFormatException e) {
                        logger.error("Error while parse long:" + value.toString(), e);
                        throw e;
                    }
                } else if (value instanceof Date) {
                    return ((Date) value).getTime();
                } else if (value instanceof Time) {
                    return ((Time) value).getTime();
                } else if (value instanceof java.util.Date) {
                    return ((java.util.Date) value).getTime();
                } else if (value instanceof Boolean) {
                    Boolean ob =  ((Boolean)value);
                    return ob ? 1L : 0L;
                }
                return null;
            case BINARY:
                if (value instanceof byte[]) {
                    return value;
                } else if (value instanceof Blob) {
                    Blob item = ((Blob) value);
                    int length;
                    try {
                        length = (int) item.length();
                        return item.getBytes(1, length);
                    } catch (SQLException e) {
                        logger.error("Error while convert blob to binary, blob:" + item.toString(), e);
                        throw new RuntimeException(e);
                    }
                } else if (value instanceof String) {
                    return ((String) value).getBytes(StandardCharsets.ISO_8859_1);
                } else if (value instanceof Clob) {
                    return value.toString().getBytes(StandardCharsets.ISO_8859_1);
                }
                return null;
            case BOOL:
                if (value instanceof Boolean) {
                    return value;
                } else if (value instanceof String) {
                    return !value.equals("0");
                } else if (value instanceof Number) {
                    return ((Number) value).intValue() != 0;
                }
                return null;
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number)value).doubleValue();
                } else if (value instanceof String) {
                    try {
                        return Double.parseDouble((String) value);
                    } catch (NumberFormatException e) {
                        logger.error("Error while parse double:" + value.toString(), e);
                        throw e;
                    }
                }
                return null;
            default:
                return value;
        }
    }

    /**
     * 解析配置的字段映射类型
     * @param type
     * @return
     */
    public static TablestoreFieldType getTablestoreType(String type) {
        if (type != null) {
            if (type.equalsIgnoreCase("string")) {
                return TablestoreFieldType.STRING;
            } else if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer")) {
                return TablestoreFieldType.INT;
            } else if (type.equalsIgnoreCase("bool") || type.equalsIgnoreCase("boolean")) {
                return TablestoreFieldType.BOOL;
            } else if (type.equalsIgnoreCase("binary")) {
                return TablestoreFieldType.BINARY;
            } else if (type.equalsIgnoreCase("double") || type.equalsIgnoreCase("float") || type.equalsIgnoreCase("decimal")) {
                return TablestoreFieldType.DOUBLE;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }


    public static ColumnValue getColumnValue(Object value, TablestoreFieldType fieldType) {
        Object tablestoreValue;
        switch (fieldType) {
            case STRING:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return ColumnValue.fromString((String)tablestoreValue);
            case INT:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return ColumnValue.fromLong((long)tablestoreValue);
            case BINARY:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return ColumnValue.fromBinary((byte[])tablestoreValue);
            case DOUBLE:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return ColumnValue.fromDouble((double)tablestoreValue);
            case BOOL:
                tablestoreValue = getTablestoreValue(value, fieldType);
                return ColumnValue.fromBoolean((boolean)tablestoreValue);
            default:
                return ColumnValue.fromString(value.toString());
        }


    }

    public static Map<String, MappingConfig.ColumnItem> getTypeMap(MappingConfig config) {
        return config.getDbMapping().getColumnItems();
    }

    public static TablestoreFieldType getDefaultTablestoreType(int sqlType) {
        switch (sqlType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return TablestoreFieldType.BOOL;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return TablestoreFieldType.BINARY;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return TablestoreFieldType.INT;
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return TablestoreFieldType.DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return TablestoreFieldType.STRING;
            default:
                return TablestoreFieldType.STRING;
        }
    }

    public static String getDbTableName(String db,String table) {
        String result = "";
        if (StringUtils.isNotEmpty(db)) {
            result += ("`" + db + "`.");
        }
        result += ("`" + table + "`");
        return result;
    }
}

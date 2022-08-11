package com.alibaba.otter.canal.client.adapter.es.core.support;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ES 同步工具同类
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncUtil {

    private static Logger logger = LoggerFactory.getLogger(ESSyncUtil.class);

    public static Object convertToEsObj(Object val, String fieldInfo) {
        if (val == null) {
            return null;
        }
        if (fieldInfo.startsWith("array:")) {
            String separator = fieldInfo.substring("array:".length()).trim();
            String[] values = val.toString().split(separator);
            return Arrays.asList(values);
        } else if (fieldInfo.startsWith("object")) {
            if (val instanceof String){
                return JSON.parse(val.toString());
            }
            return JSON.parse(new String((byte[])val));
        }
        return null;
    }

    /**
     * 类型转换为Mapping中对应的类型
     */
    public static Object typeConvert(Object val, String esType) {
        if (val == null) {
            return null;
        }
        if (esType == null) {
            return val;
        }
        Object res = null;
        switch (esType) {
            case "integer":
                if (val instanceof Number) {
                    res = ((Number) val).intValue();
                } else {
                    res = Integer.parseInt(val.toString());
                }
                break;
            case "long":
                if (val instanceof Number) {
                    res = ((Number) val).longValue();
                } else {
                    res = Long.parseLong(val.toString());
                }
                break;
            case "short":
                if (val instanceof Number) {
                    res = ((Number) val).shortValue();
                } else {
                    res = Short.parseShort(val.toString());
                }
                break;
            case "byte":
                if (val instanceof Number) {
                    res = ((Number) val).byteValue();
                } else {
                    res = Byte.parseByte(val.toString());
                }
                break;
            case "double":
                if (val instanceof Number) {
                    res = ((Number) val).doubleValue();
                } else {
                    res = Double.parseDouble(val.toString());
                }
                break;
            case "float":
            case "half_float":
            case "scaled_float":
                if (val instanceof Number) {
                    res = ((Number) val).floatValue();
                } else {
                    res = Float.parseFloat(val.toString());
                }
                break;
            case "boolean":
                if (val instanceof Boolean) {
                    res = val;
                } else if (val instanceof Number) {
                    int v = ((Number) val).intValue();
                    res = v != 0;
                } else {
                    res = Boolean.parseBoolean(val.toString());
                }
                break;
            case "date":
                if (val instanceof java.sql.Time) {
                    DateTime dateTime = new DateTime(((java.sql.Time) val).getTime());
                    if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("HH:mm:ss.SSS");
                    } else {
                        res = dateTime.toString("HH:mm:ss");
                    }
                } else if (val instanceof java.sql.Timestamp) {
                    DateTime dateTime = new DateTime(((java.sql.Timestamp) val).getTime());
                    if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                    } else {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                    }
                } else if (val instanceof java.sql.Date || val instanceof Date) {
                    DateTime dateTime;
                    if (val instanceof java.sql.Date) {
                        dateTime = new DateTime(((java.sql.Date) val).getTime());
                    } else {
                        dateTime = new DateTime(((Date) val).getTime());
                    }
                    if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                            && dateTime.getMillisOfSecond() == 0) {
                        res = dateTime.toString("yyyy-MM-dd");
                    } else {
                        if (dateTime.getMillisOfSecond() != 0) {
                            res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                        } else {
                            res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                        }
                    }
                } else if (val instanceof Long) {
                    DateTime dateTime = new DateTime(((Long) val).longValue());
                    if (dateTime.getHourOfDay() == 0 && dateTime.getMinuteOfHour() == 0 && dateTime.getSecondOfMinute() == 0
                            && dateTime.getMillisOfSecond() == 0) {
                        res = dateTime.toString("yyyy-MM-dd");
                    } else if (dateTime.getMillisOfSecond() != 0) {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                    } else {
                        res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                    }
                } else if (val instanceof String) {
                    String v = ((String) val).trim();
                    if (v.length() > 18 && v.charAt(4) == '-' && v.charAt(7) == '-' && v.charAt(10) == ' '
                            && v.charAt(13) == ':' && v.charAt(16) == ':') {
                        String dt = v.substring(0, 10) + "T" + v.substring(11);
                        Date date = Util.parseDate(dt);
                        if (date != null) {
                            DateTime dateTime = new DateTime(date);
                            if (dateTime.getMillisOfSecond() != 0) {
                                res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSS" + Util.timeZone);
                            } else {
                                res = dateTime.toString("yyyy-MM-dd'T'HH:mm:ss" + Util.timeZone);
                            }
                        }
                    } else if (v.length() == 10 && v.charAt(4) == '-' && v.charAt(7) == '-') {
                        Date date = Util.parseDate(v);
                        if (date != null) {
                            DateTime dateTime = new DateTime(date);
                            res = dateTime.toString("yyyy-MM-dd");
                        }
                    }
                }
                break;
            case "binary":
                if (val instanceof byte[]) {
                    Base64 base64 = new Base64();
                    res = base64.encodeAsString((byte[]) val);
                } else if (val instanceof Blob) {
                    byte[] b = blobToBytes((Blob) val);
                    Base64 base64 = new Base64();
                    res = base64.encodeAsString(b);
                } else if (val instanceof String) {
                    // 对应canal中的单字节编码
                    byte[] b = ((String) val).getBytes(StandardCharsets.ISO_8859_1);
                    Base64 base64 = new Base64();
                    res = base64.encodeAsString(b);
                }
                break;
            case "geo_point":
                if (!(val instanceof String)) {
                    logger.error("es type is geo_point, but source type is not String");
                    return val;
                }

                if (!((String) val).contains(",")) {
                    logger.error("es type is geo_point, source value not contains ',' separator");
                    return val;
                }

                String[] point = ((String) val).split(",");
                Map<String, Double> location = new HashMap<>();
                location.put("lat", Double.valueOf(point[0].trim()));
                location.put("lon", Double.valueOf(point[1].trim()));
                return location;
            case "array":
                if ("".equals(val.toString().trim())) {
                    res = new ArrayList<>();
                } else {
                    String value = val.toString();
                    String separator = ",";
                    if (!value.contains(",")) {
                        if (value.contains(";")) {
                            separator = ";";
                        } else if (value.contains("|")) {
                            separator = "\\|";
                        } else if (value.contains("-")) {
                            separator = "-";
                        }
                    }
                    String[] values = value.split(separator);
                    return Arrays.asList(values);
                }
                break;
            case "object":
                if ("".equals(val.toString().trim())) {
                    res = new HashMap<>();
                } else {
                    res = JSON.parseObject(val.toString(), Map.class);
                }
                break;
            default:
                // 其他类全以字符串处理
                res = val.toString();
                break;
        }

        return res;
    }

    /**
     * Blob转byte[]
     */
    private static byte[] blobToBytes(Blob blob) {
        try (InputStream is = blob.getBinaryStream()) {
            byte[] b = new byte[(int) blob.length()];
            if (is.read(b) != -1) {
                return b;
            } else {
                return new byte[0];
            }
        } catch (IOException | SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    /**
     * 拼接主键条件
     *
     * @param mapping
     * @param data
     * @return
     */
    public static String pkConditionSql(ESMapping mapping, Map<String, Object> data) {
        Set<ColumnItem> idColumns = new LinkedHashSet<>();
        SchemaItem schemaItem = mapping.getSchemaItem();

        TableItem mainTable = schemaItem.getMainTable();

        for (ColumnItem idColumnItem : schemaItem.getIdFieldItem(mapping).getColumnItems()) {
            if ((mainTable.getAlias() == null && idColumnItem.getOwner() == null)
                || (mainTable.getAlias() != null && mainTable.getAlias().equals(idColumnItem.getOwner()))) {
                idColumns.add(idColumnItem);
            }
        }

        if (idColumns.isEmpty()) {
            throw new RuntimeException("Not found primary key field in main table");
        }

        // 拼接condition
        StringBuilder condition = new StringBuilder(" ");
        for (ColumnItem idColumn : idColumns) {
            Object idVal = data.get(idColumn.getColumnName());
            if (mainTable.getAlias() != null) condition.append(mainTable.getAlias()).append(".");
            condition.append(idColumn.getColumnName()).append("=");
            if (idVal instanceof String) {
                condition.append("'").append(idVal).append("' AND ");
            } else {
                condition.append(idVal).append(" AND ");
            }
        }

        if (condition.toString().endsWith("AND ")) {
            int len2 = condition.length();
            condition.delete(len2 - 4, len2);
        }
        return condition.toString();
    }

    public static String appendCondition(String sql, String condition) {
        return sql + " WHERE " + condition + " ";
    }

    public static void appendCondition(StringBuilder sql, Object value, String owner, String columnName) {
        if (value instanceof String) {
            sql.append(owner).append(".").append(columnName).append("='").append(value).append("'  AND ");
        } else {
            sql.append(owner).append(".").append(columnName).append("=").append(value).append("  AND ");
        }
    }
}

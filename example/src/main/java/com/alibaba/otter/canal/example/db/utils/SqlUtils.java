package com.alibaba.otter.canal.example.db.utils;

import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SqlUtils {

    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";
    public static final String SQLDATE_FORMAT = "yyyy-MM-dd";
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final Map<Integer, Class<?>> sqlTypeToJavaTypeMap = new HashMap<Integer, Class<?>>();
    private static final ConvertUtilsBean convertUtilsBean = new ConvertUtilsBean();

    static {
        // regist Converter
        convertUtilsBean.register(SqlTimestampConverter.SQL_TIMESTAMP, Date.class);
        convertUtilsBean.register(SqlTimestampConverter.SQL_TIMESTAMP, Time.class);
        convertUtilsBean.register(SqlTimestampConverter.SQL_TIMESTAMP, Timestamp.class);
        convertUtilsBean.register(ByteArrayConverter.SQL_BYTES, byte[].class);

        // bool
        sqlTypeToJavaTypeMap.put(Types.BOOLEAN, Boolean.class);

        // int
        sqlTypeToJavaTypeMap.put(Types.TINYINT, Integer.class);
        sqlTypeToJavaTypeMap.put(Types.SMALLINT, Integer.class);
        sqlTypeToJavaTypeMap.put(Types.INTEGER, Integer.class);

        // long
        sqlTypeToJavaTypeMap.put(Types.BIGINT, Long.class);
        // mysql bit最多64位，无符号
        sqlTypeToJavaTypeMap.put(Types.BIT, BigInteger.class);

        // decimal
        sqlTypeToJavaTypeMap.put(Types.REAL, Float.class);
        sqlTypeToJavaTypeMap.put(Types.FLOAT, Float.class);
        sqlTypeToJavaTypeMap.put(Types.DOUBLE, Double.class);
        sqlTypeToJavaTypeMap.put(Types.NUMERIC, BigDecimal.class);
        sqlTypeToJavaTypeMap.put(Types.DECIMAL, BigDecimal.class);

        // date
        sqlTypeToJavaTypeMap.put(Types.DATE, Date.class);
        sqlTypeToJavaTypeMap.put(Types.TIME, Time.class);
        sqlTypeToJavaTypeMap.put(Types.TIMESTAMP, Timestamp.class);

        // blob
        sqlTypeToJavaTypeMap.put(Types.BLOB, byte[].class);

        // byte[]
        sqlTypeToJavaTypeMap.put(Types.REF, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.OTHER, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.ARRAY, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.STRUCT, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.SQLXML, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.BINARY, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.DATALINK, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.DISTINCT, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.VARBINARY, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.JAVA_OBJECT, byte[].class);
        sqlTypeToJavaTypeMap.put(Types.LONGVARBINARY, byte[].class);

        // String
        sqlTypeToJavaTypeMap.put(Types.CHAR, String.class);
        sqlTypeToJavaTypeMap.put(Types.VARCHAR, String.class);
        sqlTypeToJavaTypeMap.put(Types.LONGVARCHAR, String.class);
        sqlTypeToJavaTypeMap.put(Types.LONGNVARCHAR, String.class);
        sqlTypeToJavaTypeMap.put(Types.NCHAR, String.class);
        sqlTypeToJavaTypeMap.put(Types.NVARCHAR, String.class);
        sqlTypeToJavaTypeMap.put(Types.NCLOB, String.class);
        sqlTypeToJavaTypeMap.put(Types.CLOB, String.class);
    }

    /**
     * 将指定java.sql.Types的ResultSet value转换成相应的String
     *
     * @param rs
     * @param index
     * @param sqlType
     * @return
     * @throws SQLException
     */
    public static String sqlValueToString(ResultSet rs, int index, int sqlType) throws SQLException {
        Class<?> requiredType = sqlTypeToJavaTypeMap.get(sqlType);
        if (requiredType == null) {
            throw new IllegalArgumentException("unknow java.sql.Types - " + sqlType);
        }

        return getResultSetValue(rs, index, requiredType);
    }

    /**
     * sqlValueToString方法的逆向过程
     *
     * @param value
     * @param sqlType
     * @param isTextRequired
     * @param isEmptyStringNulled
     * @return
     */
    public static Object stringToSqlValue(String value, int sqlType, boolean isRequired, boolean isEmptyStringNulled) {
        // 设置变量
        String sourceValue = value;
        if (SqlUtils.isTextType(sqlType)) {
            if ((sourceValue == null) || (StringUtils.isEmpty(sourceValue) && isEmptyStringNulled)) {
                return isRequired ? REQUIRED_FIELD_NULL_SUBSTITUTE : null;
            } else {
                return sourceValue;
            }
        } else {
            if (StringUtils.isEmpty(sourceValue)) {
                return null;
            } else {
                Class<?> requiredType = sqlTypeToJavaTypeMap.get(sqlType);
                if (requiredType == null) {
                    throw new IllegalArgumentException("unknow java.sql.Types - " + sqlType);
                } else if (requiredType.equals(String.class)) {
                    return sourceValue;
                } else if (isNumeric(sqlType)) {
                    return convertUtilsBean.convert(sourceValue.trim(), requiredType);
                } else {
                    return convertUtilsBean.convert(sourceValue, requiredType);
                }
            }
        }
    }

    public static String encoding(String source, int sqlType, String sourceEncoding, String targetEncoding) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                if (false == StringUtils.isEmpty(source)) {
                    String fromEncoding = StringUtils.isBlank(sourceEncoding) ? "UTF-8" : sourceEncoding;
                    String toEncoding = StringUtils.isBlank(targetEncoding) ? "UTF-8" : targetEncoding;

                    // if (false == StringUtils.equalsIgnoreCase(fromEncoding,
                    // toEncoding)) {
                    try {
                        return new String(source.getBytes(fromEncoding), toEncoding);
                    } catch (UnsupportedEncodingException e) {
                        throw new IllegalArgumentException(e.getMessage(), e);
                    }
                    // }
                }
        }

        return source;
    }

    /**
     * Retrieve a JDBC column value from a ResultSet, using the specified value
     * type.
     * <p>
     * Uses the specifically typed ResultSet accessor methods, falling back to
     * {@link #getResultSetValue(ResultSet, int)} for unknown types.
     * <p>
     * Note that the returned value may not be assignable to the specified
     * required type, in case of an unknown type. Calling code needs to deal
     * with this case appropriately, e.g. throwing a corresponding exception.
     *
     * @param rs           is the ResultSet holding the data
     * @param index        is the column index
     * @param requiredType the required value type (may be <code>null</code>)
     * @return the value object
     * @throws SQLException if thrown by the JDBC API
     */
    private static String getResultSetValue(ResultSet rs, int index, Class<?> requiredType) throws SQLException {
        if (requiredType == null) {
            return getResultSetValue(rs, index);
        }

        Object value = null;
        boolean wasNullCheck = false;

        // Explicitly extract typed value, as far as possible.
        if (String.class.equals(requiredType)) {
            value = rs.getString(index);
        } else if (boolean.class.equals(requiredType) || Boolean.class.equals(requiredType)) {
            value = Boolean.valueOf(rs.getBoolean(index));
            wasNullCheck = true;
        } else if (byte.class.equals(requiredType) || Byte.class.equals(requiredType)) {
            value = new Byte(rs.getByte(index));
            wasNullCheck = true;
        } else if (short.class.equals(requiredType) || Short.class.equals(requiredType)) {
            value = new Short(rs.getShort(index));
            wasNullCheck = true;
        } else if (int.class.equals(requiredType) || Integer.class.equals(requiredType)) {
            value = new Long(rs.getLong(index));
            wasNullCheck = true;
        } else if (long.class.equals(requiredType) || Long.class.equals(requiredType)) {
            value = rs.getBigDecimal(index);
            wasNullCheck = true;
        } else if (float.class.equals(requiredType) || Float.class.equals(requiredType)) {
            value = new Float(rs.getFloat(index));
            wasNullCheck = true;
        } else if (double.class.equals(requiredType) || Double.class.equals(requiredType)
                || Number.class.equals(requiredType)) {
            value = new Double(rs.getDouble(index));
            wasNullCheck = true;
        } else if (Time.class.equals(requiredType)) {
            // try {
            // value = rs.getTime(index);
            // } catch (SQLException e) {
            value = rs.getString(index);// 尝试拿为string对象，0000无法用Time表示
            // if (value == null && !rs.wasNull()) {
            // value = "00:00:00"; //
            // mysql设置了zeroDateTimeBehavior=convertToNull，出现0值时返回为null
            // }
            // }
        } else if (Timestamp.class.equals(requiredType) || Date.class.equals(requiredType)) {
            // try {
            // value = convertTimestamp(rs.getTimestamp(index));
            // } catch (SQLException e) {
            // 尝试拿为string对象，0000-00-00 00:00:00无法用Timestamp 表示
            value = rs.getString(index);
            // if (value == null && !rs.wasNull()) {
            // value = "0000:00:00 00:00:00"; //
            // mysql设置了zeroDateTimeBehavior=convertToNull，出现0值时返回为null
            // }
            // }
        } else if (BigDecimal.class.equals(requiredType)) {
            value = rs.getBigDecimal(index);
        } else if (BigInteger.class.equals(requiredType)) {
            value = rs.getBigDecimal(index);
        } else if (Blob.class.equals(requiredType)) {
            value = rs.getBlob(index);
        } else if (Clob.class.equals(requiredType)) {
            value = rs.getClob(index);
        } else if (byte[].class.equals(requiredType)) {
            try {
                byte[] bytes = rs.getBytes(index);
                if (bytes == null) {
                    value = null;
                } else {
                    value = new String(bytes, "ISO-8859-1");// 将binary转化为iso-8859-1的字符串
                }
            } catch (UnsupportedEncodingException e) {
                throw new SQLException(e);
            }
        } else {
            // Some unknown type desired -> rely on getObject.
            value = getResultSetValue(rs, index);
        }

        // Perform was-null check if demanded (for results that the
        // JDBC driver returns as primitives).
        if (wasNullCheck && (value != null) && rs.wasNull()) {
            value = null;
        }

        return (value == null) ? null : convertUtilsBean.convert(value);
    }

    /**
     * Retrieve a JDBC column value from a ResultSet, using the most appropriate
     * value type. The returned value should be a detached value object, not
     * having any ties to the active ResultSet: in particular, it should not be
     * a Blob or Clob object but rather a byte array respectively String
     * representation.
     * <p>
     * Uses the <code>getObject(index)</code> method, but includes additional
     * "hacks" to get around Oracle 10g returning a non-standard object for its
     * TIMESTAMP datatype and a <code>java.sql.Date</code> for DATE columns
     * leaving out the time portion: These columns will explicitly be extracted
     * as standard <code>java.sql.Timestamp</code> object.
     *
     * @param rs    is the ResultSet holding the data
     * @param index is the column index
     * @return the value object
     * @throws SQLException if thrown by the JDBC API
     * @see Blob
     * @see Clob
     * @see Timestamp
     */
    private static String getResultSetValue(ResultSet rs, int index) throws SQLException {
        Object obj = rs.getObject(index);
        return (obj == null) ? null : convertUtilsBean.convert(obj);
    }

    // private static Object convertTimestamp(Timestamp timestamp) {
    // return (timestamp == null) ? null : timestamp.getTime();
    // }

    /**
     * Check whether the given SQL type is numeric.
     */
    public static boolean isNumeric(int sqlType) {
        return (Types.BIT == sqlType) || (Types.BIGINT == sqlType) || (Types.DECIMAL == sqlType)
                || (Types.DOUBLE == sqlType) || (Types.FLOAT == sqlType) || (Types.INTEGER == sqlType)
                || (Types.NUMERIC == sqlType) || (Types.REAL == sqlType) || (Types.SMALLINT == sqlType)
                || (Types.TINYINT == sqlType);
    }

    public static boolean isTextType(int sqlType) {
        if (sqlType == Types.CHAR || sqlType == Types.VARCHAR || sqlType == Types.CLOB || sqlType == Types.LONGVARCHAR
                || sqlType == Types.NCHAR || sqlType == Types.NVARCHAR || sqlType == Types.NCLOB
                || sqlType == Types.LONGNVARCHAR) {
            return true;
        } else {
            return false;
        }
    }
}

package com.alibaba.otter.canal.client.adapter.hbase.support;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Phoenix类型
 *
 * @author machengyuan 2018-8-21 下午06:12:34
 * @version 1.0.0
 */
public enum PhType {
    DEFAULT(-1, "VARCHAR"), UNSIGNED_INT(4, "UNSIGNED_INT"), UNSIGNED_LONG(8, "UNSIGNED_LONG"),
    UNSIGNED_TINYINT(1, "UNSIGNED_TINYINT"), UNSIGNED_SMALLINT(2, "UNSIGNED_SMALLINT"),
    UNSIGNED_FLOAT(4, "UNSIGNED_FLOAT"), UNSIGNED_DOUBLE(8, "UNSIGNED_DOUBLE"), INTEGER(4, "INTEGER"),
    BIGINT(8, "BIGINT"), TINYINT(1, "TINYINT"), SMALLINT(2, "SMALLINT"), FLOAT(4, "FLOAT"), DOUBLE(8, "DOUBLE"),
    DECIMAL(-1, "DECIMAL"), BOOLEAN(1, "BOOLEAN"), UNSIGNED_TIME(8, "UNSIGNED_TIME"),
    UNSIGNED_DATE(8, "UNSIGNED_DATE"), UNSIGNED_TIMESTAMP(12, "UNSIGNED_TIMESTAMP"), TIME(8, "TIME"), DATE(8, "DATE"),
    TIMESTAMP(12, "TIMESTAMP"), VARCHAR(-1, "VARCHAR"), VARBINARY(-1, "VARBINARY");

    /**
     * -1：长度可变
     */
    private int    len;
    private String type;

    PhType(int len, String type){
        this.len = len;
        this.type = type;
    }

    public int getLen() {
        return len;
    }

    public String getType() {
        return this.type;
    }

    public static PhType getType(Class<?> javaType) {
        if (javaType == null) return DEFAULT;
        PhType phType;
        if (Integer.class.isAssignableFrom(javaType) || int.class.isAssignableFrom(javaType)) {
            phType = INTEGER;
        } else if (Long.class.isAssignableFrom(javaType) || long.class.isAssignableFrom(javaType)) {
            phType = BIGINT;
        } else if (Byte.class.isAssignableFrom(javaType) || byte.class.isAssignableFrom(javaType)) {
            phType = TINYINT;
        } else if (Short.class.isAssignableFrom(javaType) || short.class.isAssignableFrom(javaType)) {
            phType = SMALLINT;
        } else if (Float.class.isAssignableFrom(javaType) || float.class.isAssignableFrom(javaType)) {
            phType = FLOAT;
        } else if (Double.class.isAssignableFrom(javaType) || double.class.isAssignableFrom(javaType)) {
            phType = DOUBLE;
        } else if (Boolean.class.isAssignableFrom(javaType) || boolean.class.isAssignableFrom(javaType)) {
            phType = BOOLEAN;
        } else if (java.sql.Date.class.isAssignableFrom(javaType)) {
            phType = DATE;
        } else if (Time.class.isAssignableFrom(javaType)) {
            phType = DATE;
        } else if (Timestamp.class.isAssignableFrom(javaType)) {
            phType = TIMESTAMP;
        } else if (Date.class.isAssignableFrom(javaType)) {
            phType = DATE;
        } else if (byte[].class.isAssignableFrom(javaType)) {
            phType = VARBINARY;
        } else if (String.class.isAssignableFrom(javaType)) {
            phType = VARCHAR;
        } else if (BigDecimal.class.isAssignableFrom(javaType)) {
            phType = DECIMAL;
        } else if (BigInteger.class.isAssignableFrom(javaType)) {
            phType = UNSIGNED_LONG;
        } else {
            phType = DEFAULT;
        }
        return phType;
    }

    public static PhType getType(String type) {
        if (type == null) return DEFAULT;
        PhType phType;
        if (type.equalsIgnoreCase(UNSIGNED_INT.type)) {
            phType = UNSIGNED_INT;
        } else if (type.equalsIgnoreCase(UNSIGNED_LONG.type)) {
            phType = UNSIGNED_LONG;
        } else if (type.equalsIgnoreCase(UNSIGNED_TINYINT.type)) {
            phType = UNSIGNED_TINYINT;
        } else if (type.equalsIgnoreCase(UNSIGNED_SMALLINT.type)) {
            phType = UNSIGNED_SMALLINT;
        } else if (type.equalsIgnoreCase(UNSIGNED_FLOAT.type)) {
            phType = UNSIGNED_FLOAT;
        } else if (type.equalsIgnoreCase(UNSIGNED_DOUBLE.type)) {
            phType = UNSIGNED_DOUBLE;
        } else if (type.equalsIgnoreCase(INTEGER.type)) {
            phType = INTEGER;
        } else if (type.equalsIgnoreCase(BIGINT.type)) {
            phType = BIGINT;
        } else if (type.equalsIgnoreCase(TINYINT.type)) {
            phType = TINYINT;
        } else if (type.equalsIgnoreCase(SMALLINT.type)) {
            phType = SMALLINT;
        } else if (type.equalsIgnoreCase(FLOAT.type)) {
            phType = FLOAT;
        } else if (type.equalsIgnoreCase(DOUBLE.type)) {
            phType = DOUBLE;
        } else if (type.equalsIgnoreCase(BOOLEAN.type)) {
            phType = BOOLEAN;
        } else if (type.equalsIgnoreCase(UNSIGNED_TIME.type)) {
            phType = UNSIGNED_TIME;
        } else if (type.equalsIgnoreCase(UNSIGNED_DATE.type)) {
            phType = UNSIGNED_DATE;
        } else if (type.equalsIgnoreCase(UNSIGNED_TIMESTAMP.type)) {
            phType = UNSIGNED_TIMESTAMP;
        } else if (type.equalsIgnoreCase(TIME.type)) {
            phType = TIME;
        } else if (type.equalsIgnoreCase(DATE.type)) {
            phType = DATE;
        } else if (type.equalsIgnoreCase(TIMESTAMP.type)) {
            phType = TIMESTAMP;
        } else if (type.equalsIgnoreCase(VARCHAR.type)) {
            phType = VARCHAR;
        } else if (type.equalsIgnoreCase(VARBINARY.type)) {
            phType = VARBINARY;
        } else if (type.equalsIgnoreCase(DECIMAL.type)) {
            phType = DECIMAL;
        } else {
            phType = DEFAULT;
        }
        return phType;
    }
}

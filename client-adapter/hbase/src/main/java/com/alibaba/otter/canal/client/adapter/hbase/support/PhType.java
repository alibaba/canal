package com.alibaba.otter.canal.client.adapter.hbase.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                    DEFAULT, UNSIGNED_INT, UNSIGNED_LONG, UNSIGNED_TINYINT, UNSIGNED_SMALLINT, UNSIGNED_FLOAT,
                    UNSIGNED_DOUBLE, INTEGER, BIGINT, TINYINT, SMALLINT, FLOAT, DOUBLE, DECIMAL, BOOLEAN, UNSIGNED_TIME,
                    UNSIGNED_DATE, UNSIGNED_TIMESTAMP, TIME, DATE, TIMESTAMP, VARCHAR, VARBINARY;

    private static Logger logger = LoggerFactory.getLogger(PhType.class);

    public static PhType getType(Class<?> javaType) {
        if (javaType == null) return DEFAULT;
        PhType phType;
        if (Integer.class == javaType || int.class == javaType) {
            phType = INTEGER;
        } else if (Long.class == javaType || long.class == javaType) {
            phType = BIGINT;
        } else if (Byte.class == javaType || byte.class == javaType) {
            phType = TINYINT;
        } else if (Short.class == javaType || short.class == javaType) {
            phType = SMALLINT;
        } else if (Float.class == javaType || float.class == javaType) {
            phType = FLOAT;
        } else if (Double.class == javaType || double.class == javaType) {
            phType = DOUBLE;
        } else if (Boolean.class == javaType || boolean.class == javaType) {
            phType = BOOLEAN;
        } else if (java.sql.Date.class == javaType) {
            phType = DATE;
        } else if (Time.class == javaType) {
            phType = DATE;
        } else if (Timestamp.class == javaType) {
            phType = TIMESTAMP;
        } else if (Date.class == javaType) {
            phType = DATE;
        } else if (byte[].class == javaType) {
            phType = VARBINARY;
        } else if (String.class == javaType) {
            phType = VARCHAR;
        } else if (BigDecimal.class == javaType) {
            phType = DECIMAL;
        } else if (BigInteger.class == javaType) {
            phType = UNSIGNED_LONG;
        } else {
            phType = DEFAULT;
        }
        return phType;
    }

    public static PhType getType(String type) {
        if (type == null) return DEFAULT;
        try {
            return PhType.valueOf(type.toUpperCase());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return DEFAULT;
        }
    }
}

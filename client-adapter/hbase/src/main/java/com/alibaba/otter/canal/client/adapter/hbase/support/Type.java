package com.alibaba.otter.canal.client.adapter.hbase.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Java类型
 *
 * @author machengyuan 2018-8-21 下午06:11:36
 * @version 1.0.0
 */
public enum Type {
                  DEFAULT, STRING, INTEGER, LONG, SHORT, BOOLEAN, FLOAT, DOUBLE, BIGDECIMAL, DATE, BYTE, BYTES;

    private static Logger logger = LoggerFactory.getLogger(Type.class);

    public static Type getType(String type) {
        if (type == null) {
            return DEFAULT;
        }
        try {
            return Type.valueOf(type);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return DEFAULT;
        }
    }

    public static Type getType(Class<?> javaType) {
        if (javaType == null) {
            return DEFAULT;
        }
        Type type;
        if (Integer.class == javaType || int.class == javaType) {
            type = INTEGER;
        } else if (Long.class == javaType || long.class == javaType) {
            type = LONG;
        } else if (Byte.class == javaType || byte.class == javaType) {
            type = BYTE;
        } else if (Short.class == javaType || short.class == javaType) {
            type = SHORT;
        } else if (Float.class == javaType || float.class == javaType) {
            type = FLOAT;
        } else if (Double.class == javaType || double.class == javaType) {
            type = DOUBLE;
        } else if (Boolean.class == javaType || boolean.class == javaType) {
            type = BOOLEAN;
        } else if (Date.class == javaType) {
            type = DATE;
        } else if (byte[].class == javaType) {
            type = BYTES;
        } else if (String.class == javaType) {
            type = STRING;
        } else if (BigDecimal.class == javaType) {
            type = BIGDECIMAL;
        } else if (BigInteger.class == javaType) {
            type = LONG;
        } else {
            type = DEFAULT;
        }
        return type;
    }
}

package com.alibaba.otter.canal.client.adapter.hbase.support;

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
    DEFAULT("STRING"), STRING("STRING"), INTEGER("INTEGER"), LONG("LONG"), SHORT("SHORT"), BOOLEAN("BOOLEAN"),
    FLOAT("FLOAT"), DOUBLE("DOUBLE"), BIGDECIMAL("BIGDECIMAL"), DATE("DATE"), BYTE("BYTE"), BYTES("BYTES");

    private String type;

    Type(String type){
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static Type getType(String type) {
        if (type == null) {
            return DEFAULT;
        }
        Type res;
        if (type.equalsIgnoreCase("STRING")) {
            res = STRING;
        } else if (type.equalsIgnoreCase("INTEGER")) {
            res = INTEGER;
        } else if (type.equalsIgnoreCase("LONG")) {
            res = LONG;
        } else if (type.equalsIgnoreCase("SHORT")) {
            res = SHORT;
        } else if (type.equalsIgnoreCase("BOOLEAN")) {
            res = BOOLEAN;
        } else if (type.equalsIgnoreCase("FLOAT")) {
            res = FLOAT;
        } else if (type.equalsIgnoreCase("DOUBLE")) {
            res = DOUBLE;
        } else if (type.equalsIgnoreCase("BIGDECIMAL")) {
            res = BIGDECIMAL;
        } else if (type.equalsIgnoreCase("DATE")) {
            res = DATE;
        } else if (type.equalsIgnoreCase("BYTE")) {
            res = BYTE;
        } else if (type.equalsIgnoreCase("BYTES")) {
            res = BYTES;
        } else {
            res = DEFAULT;
        }
        return res;
    }

    public static Type getType(Class<?> javaType) {
        if (javaType == null) {
            return DEFAULT;
        }
        Type type;
        if (Integer.class.isAssignableFrom(javaType) || int.class.isAssignableFrom(javaType)) {
            type = INTEGER;
        } else if (Long.class.isAssignableFrom(javaType) || long.class.isAssignableFrom(javaType)) {
            type = LONG;
        } else if (Byte.class.isAssignableFrom(javaType) || byte.class.isAssignableFrom(javaType)) {
            type = BYTE;
        } else if (Short.class.isAssignableFrom(javaType) || short.class.isAssignableFrom(javaType)) {
            type = SHORT;
        } else if (Float.class.isAssignableFrom(javaType) || float.class.isAssignableFrom(javaType)) {
            type = FLOAT;
        } else if (Double.class.isAssignableFrom(javaType) || double.class.isAssignableFrom(javaType)) {
            type = DOUBLE;
        } else if (Boolean.class.isAssignableFrom(javaType) || boolean.class.isAssignableFrom(javaType)) {
            type = BOOLEAN;
        } else if (Date.class.isAssignableFrom(javaType)) {
            type = DATE;
        } else if (byte[].class.isAssignableFrom(javaType)) {
            type = BYTES;
        } else if (String.class.isAssignableFrom(javaType)) {
            type = STRING;
        } else if (BigDecimal.class.isAssignableFrom(javaType)) {
            type = BIGDECIMAL;
        } else if (BigInteger.class.isAssignableFrom(javaType)) {
            type = LONG;
        } else {
            type = DEFAULT;
        }
        return type;
    }
}

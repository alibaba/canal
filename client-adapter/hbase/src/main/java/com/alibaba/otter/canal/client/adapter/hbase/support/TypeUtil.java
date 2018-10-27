package com.alibaba.otter.canal.client.adapter.hbase.support;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Java类型转换工具类
 *
 * @author machengyuan 2018-8-21 下午06:12:34
 * @version 1.0.0
 */
public class TypeUtil {

    public static byte[] toBytes(Object obj) {
        if (obj == null) {
            return null;
        }
        byte[] bytes;
        Class<?> clazz = obj.getClass();
        if (clazz == String.class) {
            bytes = Bytes.toBytes((String) obj);
        } else if (clazz == Integer.class || clazz == int.class) {
            bytes = Bytes.toBytes((Integer) obj);
        } else if (clazz == Long.class || clazz == long.class) {
            bytes = Bytes.toBytes((Long) obj);
        } else if (clazz == Short.class || clazz == short.class) {
            bytes = Bytes.toBytes((Short) obj);
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            bytes = Bytes.toBytes((Boolean) obj);
        } else if (clazz == Float.class || clazz == float.class) {
            bytes = Bytes.toBytes((Float) obj);
        } else if (clazz == Double.class || clazz == double.class) {
            bytes = Bytes.toBytes((Double) obj);
        } else if (clazz == Byte.class || clazz == byte.class) {
            bytes = new byte[] { (byte) obj };
        } else if (clazz == BigDecimal.class) {
            bytes = Bytes.toBytes((BigDecimal) obj);
        } else if (clazz == BigInteger.class) {
            bytes = Bytes.toBytes(((BigInteger) obj).longValue());
        } else if (clazz == Date.class) {
            bytes = Bytes.toBytes(((Date) obj).getTime());
        } else if (clazz == byte[].class) {
            bytes = (byte[]) obj;
        } else {
            // 其余类型统一转换为string
            bytes = Bytes.toBytes(obj.toString());
        }
        return bytes;
    }

    public static byte[] toBytes(Object v, Type type) {
        if (v == null) {
            return null;
        }
        byte[] b = null;
        if (type == Type.DEFAULT) {
            Type type1 = Type.getType(v.getClass());
            if (type1 != null && type1 != Type.DEFAULT) {
                b = toBytes(v, type1);
            }
        } else if (type == Type.STRING) {
            b = Bytes.toBytes(v.toString());
        } else if (type == Type.INTEGER) {
            b = Bytes.toBytes(((Number) v).intValue());
        } else if (type == Type.LONG) {
            b = Bytes.toBytes(((Number) v).longValue());
        } else if (type == Type.SHORT) {
            b = Bytes.toBytes(((Number) v).shortValue());
        } else if (type == Type.BYTE) {
            b = Bytes.toBytes(((Number) v).byteValue());
        } else if (type == Type.FLOAT) {
            b = Bytes.toBytes(((Number) v).floatValue());
        } else if (type == Type.DOUBLE) {
            b = Bytes.toBytes(((Number) v).doubleValue());
        } else if (type == Type.BOOLEAN) {
            b = Bytes.toBytes(((Boolean) v));
        } else if (type == Type.DATE) {
            b = Bytes.toBytes(((Date) v).getTime());
        } else if (type == Type.BYTES) {
            b = (byte[]) v;
        } else if (type == Type.BIGDECIMAL) {
            if (v instanceof BigDecimal) {
                b = Bytes.toBytes((BigDecimal) v);
            } else {
                b = Bytes.toBytes(new BigDecimal(v.toString()));
            }
        }
        return b;
    }

    @SuppressWarnings("unchecked")
    public static <T> T toObject(byte[] bytes, Class<T> clazz) {
        if (bytes == null) {
            return null;
        }
        Object res;
        if (String.class == clazz) {
            res = Bytes.toString(bytes);
        } else if (Integer.class == clazz || int.class == clazz) {
            res = Bytes.toInt(bytes);
        } else if (Long.class == clazz || long.class == clazz) {
            res = Bytes.toLong(bytes);
        } else if (Short.class == clazz || short.class == clazz) {
            res = Bytes.toShort(bytes);
        } else if (Boolean.class == clazz || boolean.class == clazz) {
            res = Bytes.toBoolean(bytes);
        } else if (Float.class == clazz || float.class == clazz) {
            res = Bytes.toFloat(bytes);
        } else if (Double.class == clazz || double.class == clazz) {
            res = Bytes.toDouble(bytes);
        } else if (Byte.class == clazz || byte.class == clazz) {
            res = bytes[0];
        } else if (BigDecimal.class == clazz) {
            res = Bytes.toBigDecimal(bytes);
        } else if (BigInteger.class == clazz) {
            res = Bytes.toLong(bytes);
        } else if (java.sql.Date.class == clazz) {
            long ts = Bytes.toLong(bytes);
            res = new java.sql.Date(ts);
        } else if (Time.class == clazz) {
            long ts = Bytes.toLong(bytes);
            res = new Time(ts);
        } else if (Timestamp.class == clazz) {
            long ts = Bytes.toLong(bytes);
            res = new Timestamp(ts);
        } else if (Date.class == clazz) {
            long ts = Bytes.toLong(bytes);
            res = new Date(ts);
        } else {
            throw new IllegalArgumentException("mismatch class type");
        }
        return (T) res;
    }

    @SuppressWarnings("unchecked")
    public static <T> T toObject(byte[] bytes, Type type) {
        if (bytes == null) {
            return null;
        }
        Object res = null;
        if (type == Type.STRING || type == Type.DEFAULT) {
            res = Bytes.toString(bytes);
        } else if (type == Type.INTEGER) {
            if (bytes.length == Bytes.SIZEOF_INT) {
                res = Bytes.toInt(bytes);
            }
        } else if (type == Type.LONG) {
            if (bytes.length == Bytes.SIZEOF_LONG) {
                res = Bytes.toLong(bytes);
            }
        } else if (type == Type.SHORT) {
            if (bytes.length == Bytes.SIZEOF_SHORT) {
                res = Bytes.toShort(bytes);
            }
        } else if (type == Type.BYTE) {
            if (bytes.length == Bytes.SIZEOF_BYTE) {
                res = bytes[0];
            }
        } else if (type == Type.FLOAT) {
            if (bytes.length == Bytes.SIZEOF_FLOAT) {
                res = Bytes.toFloat(bytes);
            }
        } else if (type == Type.DOUBLE) {
            if (bytes.length == Bytes.SIZEOF_DOUBLE) {
                res = Bytes.toDouble(bytes);
            }
        } else if (type == Type.BOOLEAN) {
            if (bytes.length == Bytes.SIZEOF_BOOLEAN) {
                res = Bytes.toBoolean(bytes);
            }
        } else if (type == Type.DATE) {
            if (bytes.length == Bytes.SIZEOF_LONG) {
                res = new Date(Bytes.toLong(bytes));
            }
        } else if (type == Type.BYTES) {
            res = bytes;
        } else if (type == Type.BIGDECIMAL) {
            res = Bytes.toBigDecimal(bytes);
        } else {
            throw new IllegalArgumentException("mismatch class type");
        }
        return (T) res;
    }
}

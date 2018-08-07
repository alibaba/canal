package com.alibaba.otter.canal.example.db.utils;

import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.converters.ArrayConverter;
import org.apache.commons.beanutils.converters.ByteConverter;

public class ByteArrayConverter implements Converter {

    public static final Converter SQL_BYTES = new ByteArrayConverter(null);
    private static final Converter converter = new ArrayConverter(byte[].class, new ByteConverter());

    protected final Object defaultValue;
    protected final boolean useDefault;

    public ByteArrayConverter() {
        this.defaultValue = null;
        this.useDefault = false;
    }

    public ByteArrayConverter(Object defaultValue) {
        this.defaultValue = defaultValue;
        this.useDefault = true;
    }

    public Object convert(Class type, Object value) {
        if (value == null) {
            if (useDefault) {
                return (defaultValue);
            } else {
                throw new ConversionException("No value specified");
            }
        }

        if (value instanceof byte[]) {
            return (value);
        }

        // BLOB类型，canal直接存储为String("ISO-8859-1")
        if (value instanceof String) {
            try {
                return ((String) value).getBytes("ISO-8859-1");
            } catch (Exception e) {
                throw new ConversionException(e);
            }
        }

        return converter.convert(type, value); // byteConvertor进行转化
    }
}

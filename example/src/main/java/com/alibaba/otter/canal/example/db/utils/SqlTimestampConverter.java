package com.alibaba.otter.canal.example.db.utils;

import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.lang.time.DateFormatUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class SqlTimestampConverter implements Converter {

    /**
     * Field description
     */
    public static final String[] DATE_FORMATS = new String[]{"yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd hh:mm:ss.fffffffff", "EEE MMM dd HH:mm:ss zzz yyyy",
            DateFormatUtils.ISO_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.SMTP_DATETIME_FORMAT.getPattern(),};

    public static final Converter SQL_TIMESTAMP = new SqlTimestampConverter(null);

    /**
     * The default value specified to our Constructor, if any.
     */
    private final Object defaultValue;

    /**
     * Should we return the default value on conversion errors?
     */
    private final boolean useDefault;

    /**
     * Create a {@link Converter} that will throw a {@link ConversionException} if a conversion error occurs.
     */
    public SqlTimestampConverter() {
        this.defaultValue = null;
        this.useDefault = false;
    }

    /**
     * Create a {@link Converter} that will return the specified default value if a conversion error occurs.
     *
     * @param defaultValue The default value to be returned
     */
    public SqlTimestampConverter(Object defaultValue) {
        this.defaultValue = defaultValue;
        this.useDefault = true;
    }

    /**
     * Convert the specified input object into an output object of the specified type.
     *
     * @param type  Data type to which this value should be converted
     * @param value The input value to be converted
     * @throws ConversionException if conversion cannot be performed successfully
     */
    public Object convert(Class type, Object value) {
        if (value == null) {
            if (useDefault) {
                return (defaultValue);
            } else {
                throw new ConversionException("No value specified");
            }
        }

        if (value instanceof java.sql.Date && java.sql.Date.class.equals(type)) {
            return value;
        } else if (value instanceof java.sql.Time && java.sql.Time.class.equals(type)) {
            return value;
        } else if (value instanceof Timestamp && Timestamp.class.equals(type)) {
            return value;
        } else {
            try {
                if (java.sql.Date.class.equals(type)) {
                    return new java.sql.Date(convertTimestamp2TimeMillis(value.toString()));
                } else if (java.sql.Time.class.equals(type)) {
                    return new java.sql.Time(convertTimestamp2TimeMillis(value.toString()));
                } else if (Timestamp.class.equals(type)) {
                    return new Timestamp(convertTimestamp2TimeMillis(value.toString()));
                } else {
                    return new Timestamp(convertTimestamp2TimeMillis(value.toString()));
                }
            } catch (Exception e) {
                throw new ConversionException("Value format invalid: " + e.getMessage(), e);
            }
        }

    }

    private Long convertTimestamp2TimeMillis(String input) {
        if (input == null) {
            return null;
        }

        try {
            // 先处理Timestamp类型
            return Timestamp.valueOf(input).getTime();
        } catch (Exception nfe) {
            try {
                try {
                    return parseDate(input, DATE_FORMATS, Locale.ENGLISH).getTime();
                } catch (Exception err) {
                    return parseDate(input, DATE_FORMATS, Locale.getDefault()).getTime();
                }
            } catch (Exception err) {
                // 最后处理long time的情况
                return Long.parseLong(input);
            }
        }
    }

    private Date parseDate(String str, String[] parsePatterns, Locale locale) throws ParseException {
        if ((str == null) || (parsePatterns == null)) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }

        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);

        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0], locale);
            } else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if ((date != null) && (pos.getIndex() == str.length())) {
                return date;
            }
        }

        throw new ParseException("Unable to parse the date: " + str, -1);
    }
}

package com.taobao.tddl.dbsync.binlog.event;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.BitSet;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Extracting JDBC type & value information from packed rows-buffer.
 * 
 * @see mysql-5.1.60/sql/log_event.cc - Rows_log_event::print_verbose_one_row
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class RowsLogBuffer
{
    protected static final Log logger = LogFactory.getLog(RowsLogBuffer.class);

    private final LogBuffer    buffer;
    private final int          columnLen;
    private final String       charsetName;
    private Calendar           cal;

    private final BitSet       nullBits;
    private int                nullBitIndex;

    private boolean            fNull;
    private int                javaType;
    private int                length;
    private Serializable       value;

    public RowsLogBuffer(LogBuffer buffer, final int columnLen,
            String charsetName)
    {
        this.buffer = buffer;
        this.columnLen = columnLen;
        this.charsetName = charsetName;
        this.nullBits = new BitSet(columnLen);
    }

    /**
     * Extracting next row from packed buffer.
     * 
     * @see mysql-5.1.60/sql/log_event.cc -
     *      Rows_log_event::print_verbose_one_row
     */
    public final boolean nextOneRow(BitSet columns)
    {
        final boolean hasOneRow = buffer.hasRemaining();

        if (hasOneRow)
        {
            int column = 0;

            for (int i = 0; i < columnLen; i++)
                if (columns.get(i))
                    column++;

            nullBitIndex = 0;
            nullBits.clear();
            buffer.fillBitmap(nullBits, column);
        }
        return hasOneRow;
    }

    /**
     * Extracting next field value from packed buffer.
     * 
     * @see mysql-5.1.60/sql/log_event.cc -
     *      Rows_log_event::print_verbose_one_row
     */
    public final Serializable nextValue(final int type, final int meta)
    {
        fNull = nullBits.get(nullBitIndex++);

        if (fNull)
        {
            value = null;
            javaType = mysqlToJavaType(type, meta);
            length = 0;
            return null;
        }
        else
        {
            // Extracting field value from packed buffer.
            return fetchValue(type, meta);
        }
    }

    /**
     * Maps the given MySQL type to the correct JDBC type.
     */
    static int mysqlToJavaType(int type, final int meta)
    {
        int javaType;

        if (type == LogEvent.MYSQL_TYPE_STRING)
        {
            if (meta >= 256)
            {
                int byte0 = meta >> 8;
                if ((byte0 & 0x30) != 0x30)
                {
                    /* a long CHAR() field: see #37426 */
                    type = byte0 | 0x30;
                }
                else
                {
                    switch (byte0)
                    {
                    case LogEvent.MYSQL_TYPE_SET:
                    case LogEvent.MYSQL_TYPE_ENUM:
                    case LogEvent.MYSQL_TYPE_STRING:
                        type = byte0;
                    }
                }
            }
        }

        switch (type)
        {
        case LogEvent.MYSQL_TYPE_LONG:
            javaType = Types.INTEGER;
            break;

        case LogEvent.MYSQL_TYPE_TINY:
            javaType = Types.TINYINT;
            break;

        case LogEvent.MYSQL_TYPE_SHORT:
            javaType = Types.SMALLINT;
            break;

        case LogEvent.MYSQL_TYPE_INT24:
            javaType = Types.INTEGER;
            break;

        case LogEvent.MYSQL_TYPE_LONGLONG:
            javaType = Types.BIGINT;
            break;

        case LogEvent.MYSQL_TYPE_DECIMAL:
            javaType = Types.DECIMAL;
            break;

        case LogEvent.MYSQL_TYPE_NEWDECIMAL:
            javaType = Types.DECIMAL;
            break;

        case LogEvent.MYSQL_TYPE_FLOAT:
            javaType = Types.REAL; // Types.FLOAT;
            break;

        case LogEvent.MYSQL_TYPE_DOUBLE:
            javaType = Types.DOUBLE;
            break;

        case LogEvent.MYSQL_TYPE_BIT:
            javaType = Types.BIT;
            break;

        case LogEvent.MYSQL_TYPE_TIMESTAMP:
        case LogEvent.MYSQL_TYPE_DATETIME:
            javaType = Types.TIMESTAMP;
            break;

        case LogEvent.MYSQL_TYPE_TIME:
            javaType = Types.TIME;
            break;

        case LogEvent.MYSQL_TYPE_NEWDATE:
        case LogEvent.MYSQL_TYPE_DATE:
        case LogEvent.MYSQL_TYPE_YEAR:
            javaType = Types.DATE;
            break;

        case LogEvent.MYSQL_TYPE_ENUM:
            javaType = Types.INTEGER;
            break;

        case LogEvent.MYSQL_TYPE_SET:
            javaType = Types.BINARY;
            break;

        case LogEvent.MYSQL_TYPE_TINY_BLOB:
        case LogEvent.MYSQL_TYPE_MEDIUM_BLOB:
        case LogEvent.MYSQL_TYPE_LONG_BLOB:
        case LogEvent.MYSQL_TYPE_BLOB:
            if (meta == 1)
            {
                javaType = Types.VARBINARY;
            }
            else
            {
                javaType = Types.LONGVARBINARY;
            }
            break;

        case LogEvent.MYSQL_TYPE_VARCHAR:
        case LogEvent.MYSQL_TYPE_VAR_STRING:
            javaType = Types.VARCHAR;
            break;

        case LogEvent.MYSQL_TYPE_STRING:
            javaType = Types.CHAR;
            break;

        case LogEvent.MYSQL_TYPE_GEOMETRY:
            javaType = Types.BINARY;
            break;

        default:
            javaType = Types.OTHER;
        }

        return javaType;
    }

    /**
     * Extracting next field value from packed buffer.
     * 
     * @see mysql-5.1.60/sql/log_event.cc - log_event_print_value
     */
    final Serializable fetchValue(int type, final int meta)
    {
        int len = 0;

        if (type == LogEvent.MYSQL_TYPE_STRING)
        {
            if (meta >= 256)
            {
                int byte0 = meta >> 8;
                int byte1 = meta & 0xff;
                if ((byte0 & 0x30) != 0x30)
                {
                    /* a long CHAR() field: see #37426 */
                    len = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                    type = byte0 | 0x30;
                }
                else
                {
                    switch (byte0)
                    {
                    case LogEvent.MYSQL_TYPE_SET:
                    case LogEvent.MYSQL_TYPE_ENUM:
                    case LogEvent.MYSQL_TYPE_STRING:
                        type = byte0;
                        len = byte1;
                        break;
                    default:
                        throw new IllegalArgumentException(
                                String.format(
                                        "!! Don't know how to handle column type=%d meta=%d (%04X)",
                                        type, meta, meta));
                    }
                }
            }
            else
                len = meta;
        }

        switch (type)
        {
        case LogEvent.MYSQL_TYPE_LONG:
            {
                // XXX: How to check signed / unsigned?
                // value = unsigned ? Long.valueOf(buffer.getUint32()) : Integer.valueOf(buffer.getInt32());
                value = Integer.valueOf(buffer.getInt32());
                javaType = Types.INTEGER;
                length = 4;
                break;
            }
        case LogEvent.MYSQL_TYPE_TINY:
            {
                // XXX: How to check signed / unsigned?
                // value = Integer.valueOf(unsigned ? buffer.getUint8() : buffer.getInt8());
                value = Integer.valueOf(buffer.getInt8());
                javaType = Types.TINYINT; // java.sql.Types.INTEGER;
                length = 1;
                break;
            }
        case LogEvent.MYSQL_TYPE_SHORT:
            {
                // XXX: How to check signed / unsigned?
                // value = Integer.valueOf(unsigned ? buffer.getUint16() : buffer.getInt16());
                value = Integer.valueOf((short) buffer.getInt16());
                javaType = Types.SMALLINT; // java.sql.Types.INTEGER;
                length = 2;
                break;
            }
        case LogEvent.MYSQL_TYPE_INT24:
            {
                // XXX: How to check signed / unsigned?
                // value = Integer.valueOf(unsigned ? buffer.getUint24() : buffer.getInt24());
                value = Integer.valueOf(buffer.getInt24());
                javaType = Types.INTEGER;
                length = 3;
                break;
            }
        case LogEvent.MYSQL_TYPE_LONGLONG:
            {
                // XXX: How to check signed / unsigned?
                // value = unsigned ? buffer.getUlong64()) : Long.valueOf(buffer.getLong64());
                value = Long.valueOf(buffer.getLong64());
                javaType = Types.BIGINT; // Types.INTEGER;
                length = 8;
                break;
            }
        case LogEvent.MYSQL_TYPE_DECIMAL:
            {
                /*
                 * log_event.h : This enumeration value is only used internally and
                 * cannot exist in a binlog.
                 */
                logger.warn("MYSQL_TYPE_DECIMAL : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
                javaType = Types.DECIMAL;
                value = null; /* unknown format */
                length = 0;
                break;
            }
        case LogEvent.MYSQL_TYPE_NEWDECIMAL:
            {
                final int precision = meta >> 8;
                final int decimals = meta & 0xff;
                value = buffer.getDecimal(precision, decimals);
                javaType = Types.DECIMAL;
                length = precision;
                break;
            }
        case LogEvent.MYSQL_TYPE_FLOAT:
            {
                value = Float.valueOf(buffer.getFloat32());
                javaType = Types.REAL; // Types.FLOAT;
                length = 4;
                break;
            }
        case LogEvent.MYSQL_TYPE_DOUBLE:
            {
                value = Double.valueOf(buffer.getDouble64());
                javaType = Types.DOUBLE;
                length = 8;
                break;
            }
        case LogEvent.MYSQL_TYPE_BIT:
            {
                /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
                final int nbits = ((meta >> 8) * 8) + (meta & 0xff);
                len = (nbits + 7) / 8;
                if (nbits > 1)
                {
                    byte[] bits = new byte[len];
                    buffer.fillBytes(bits, 0, len);
                    value = bits;
                }
                else
                {
                    final int bit = buffer.getInt8();
                    value = (bit != 0) ? Boolean.TRUE : Boolean.FALSE;
                }
                javaType = Types.BIT;
                length = nbits;
                break;
            }
        case LogEvent.MYSQL_TYPE_TIMESTAMP:
            {
                final long i32 = buffer.getUint32();
                value = new Timestamp(i32 * 1000);
                javaType = Types.TIMESTAMP;
                length = 4;
                break;
            }
        case LogEvent.MYSQL_TYPE_DATETIME:
            {
                final long i64 = buffer.getLong64(); /* YYYYMMDDhhmmss */
                final int d = (int) (i64 / 1000000);
                final int t = (int) (i64 % 1000000);
                if (cal == null)
                    cal = Calendar.getInstance();
                cal.clear();
                /* month is 0-based, 0 for january. */
                cal.set(d / 10000, (d % 10000) / 100 - 1, d % 100, t / 10000,
                        (t % 10000) / 100, t % 100);
                value = new Timestamp(cal.getTimeInMillis());
                javaType = Types.TIMESTAMP;
                length = 8;
                break;
            }
        case LogEvent.MYSQL_TYPE_TIME:
            {
                final int i32 = buffer.getUint24();
                if (cal == null)
                    cal = Calendar.getInstance();
                cal.clear();
                cal.set(70, 0, 1, i32 / 10000, (i32 % 10000) / 100, i32 % 100);
                value = new Time(cal.getTimeInMillis());
                javaType = Types.TIME;
                length = 3;
                break;
            }
        case LogEvent.MYSQL_TYPE_NEWDATE:
            {
                /*
                 * log_event.h : This enumeration value is only used internally and
                 * cannot exist in a binlog.
                 */
                logger.warn("MYSQL_TYPE_NEWDATE : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
                javaType = Types.DATE;
                value = null; /* unknown format */
                length = 0;
                break;
            }
        case LogEvent.MYSQL_TYPE_DATE:
            {
                final int i32 = buffer.getUint24();
                if (cal == null)
                    cal = Calendar.getInstance();
                cal.clear();
                /* month is 0-based, 0 for january. */
                cal.set((i32 / (16 * 32)), (i32 / 32 % 16) - 1, (i32 % 32));
                value = new java.sql.Date(cal.getTimeInMillis());
                javaType = Types.DATE;
                length = 3;
                break;
            }
        case LogEvent.MYSQL_TYPE_YEAR:
            {
                final int i32 = buffer.getUint8();
                // If connection property 'YearIsDateType' has
                // set, value is java.sql.Date.
                /*
                if (cal == null)
                    cal = Calendar.getInstance();
                cal.clear();
                cal.set(Calendar.YEAR, i32 + 1900);
                value = new java.sql.Date(cal.getTimeInMillis());
                */
                // The else, value is java.lang.Short.
                value = Short.valueOf((short) (i32 + 1900));
                // It might seem more correct to create a java.sql.Types.DATE value
                // for this date, but it is much simpler to pass the value as an
                // integer. The MySQL JDBC specification states that one can
                // pass a java int between 1901 and 2055. Creating a DATE value
                // causes truncation errors with certain SQL_MODES (e.g."STRICT_TRANS_TABLES").
                javaType = Types.INTEGER; // Types.INTEGER;
                length = 1;
                break;
            }
        case LogEvent.MYSQL_TYPE_ENUM:
            {
                final int int32;
                /*
                 * log_event.h : This enumeration value is only used internally and
                 * cannot exist in a binlog.
                 */
                switch (len)
                {
                case 1:
                    int32 = buffer.getUint8();
                    break;
                case 2:
                    int32 = buffer.getUint16();
                    break;
                default:
                    throw new IllegalArgumentException(
                            "!! Unknown ENUM packlen = " + len);
                }
                logger.warn("MYSQL_TYPE_ENUM : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
                value = Integer.valueOf(int32);
                javaType = Types.INTEGER;
                length = len;
                break;
            }
        case LogEvent.MYSQL_TYPE_SET:
            {
                /*
                 * log_event.h : This enumeration value is only used internally and
                 * cannot exist in a binlog.
                 */
                byte[] nbits = new byte[len];
                buffer.fillBytes(nbits, 0, len);
                logger.warn("MYSQL_TYPE_SET : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
                value = nbits;
                javaType = Types.BINARY; // Types.INTEGER;
                length = len;
                break;
            }
        case LogEvent.MYSQL_TYPE_TINY_BLOB:
            {
                /*
                 * log_event.h : This enumeration value is only used internally and
                 * cannot exist in a binlog.
                 */
                logger.warn("MYSQL_TYPE_TINY_BLOB : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
            }
        case LogEvent.MYSQL_TYPE_MEDIUM_BLOB:
            {
                /*
                 * log_event.h : This enumeration value is only used internally and
                 * cannot exist in a binlog.
                 */
                logger.warn("MYSQL_TYPE_MEDIUM_BLOB : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
            }
        case LogEvent.MYSQL_TYPE_LONG_BLOB:
            {
                /*
                 * log_event.h : This enumeration value is only used internally and
                 * cannot exist in a binlog.
                 */
                logger.warn("MYSQL_TYPE_LONG_BLOB : This enumeration value is "
                        + "only used internally and cannot exist in a binlog!");
            }
        case LogEvent.MYSQL_TYPE_BLOB:
            {
                /*
                 * BLOB or TEXT datatype
                 */
                switch (meta)
                {
                case 1:
                    {
                        /* TINYBLOB/TINYTEXT */
                        final int len8 = buffer.getUint8();
                        byte[] binary = new byte[len8];
                        buffer.fillBytes(binary, 0, len8);
                        value = binary;
                        javaType = Types.VARBINARY;
                        length = len8;
                        break;
                    }
                case 2:
                    {
                        /* BLOB/TEXT */
                        final int len16 = buffer.getUint16();
                        byte[] binary = new byte[len16];
                        buffer.fillBytes(binary, 0, len16);
                        value = binary;
                        javaType = Types.LONGVARBINARY;
                        length = len16;
                        break;
                    }
                case 3:
                    {
                        /* MEDIUMBLOB/MEDIUMTEXT */
                        final int len24 = buffer.getUint24();
                        byte[] binary = new byte[len24];
                        buffer.fillBytes(binary, 0, len24);
                        value = binary;
                        javaType = Types.LONGVARBINARY;
                        length = len24;
                        break;
                    }
                case 4:
                    {
                        /* LONGBLOB/LONGTEXT */
                        final int len32 = (int) buffer.getUint32();
                        byte[] binary = new byte[len32];
                        buffer.fillBytes(binary, 0, len32);
                        value = binary;
                        javaType = Types.LONGVARBINARY;
                        length = len32;
                        break;
                    }
                default:
                    throw new IllegalArgumentException(
                            "!! Unknown BLOB packlen = " + meta);
                }
                break;
            }
        case LogEvent.MYSQL_TYPE_VARCHAR:
        case LogEvent.MYSQL_TYPE_VAR_STRING:
            {
                /*
                 * Except for the data length calculation, MYSQL_TYPE_VARCHAR,
                 * MYSQL_TYPE_VAR_STRING and MYSQL_TYPE_STRING are handled the
                 * same way.
                 */
                len = meta;
                if (len < 256)
                {
                    len = buffer.getUint8();
                }
                else
                {
                    len = buffer.getUint16();
                }
                value = buffer.getFullString(len, charsetName);
                javaType = Types.VARCHAR;
                length = len;
                break;
            }
        case LogEvent.MYSQL_TYPE_STRING:
            {
                if (len < 256)
                {
                    len = buffer.getUint8();
                }
                else
                {
                    len = buffer.getUint16();
                }
                value = buffer.getFullString(len, charsetName);
                javaType = Types.CHAR; // Types.VARCHAR;
                length = len;
                break;
            }
        case LogEvent.MYSQL_TYPE_GEOMETRY:
            {
                /*
                 * MYSQL_TYPE_GEOMETRY: copy from BLOB or TEXT
                 */
                switch (meta)
                {
                case 1:
                    len = buffer.getUint8();
                    break;
                case 2:
                    len = buffer.getUint16();
                    break;
                case 3:
                    len = buffer.getUint24();
                    break;
                case 4:
                    len = (int) buffer.getUint32();
                    break;
                default:
                    throw new IllegalArgumentException(
                            "!! Unknown MYSQL_TYPE_GEOMETRY packlen = " + meta);
                }
                /* fill binary */
                byte[] binary = new byte[len];
                buffer.fillBytes(binary, 0, len);

                /* Warning unsupport cloumn type */
                logger.warn(String.format(
                        "!! Unsupport column type MYSQL_TYPE_GEOMETRY: meta=%d (%04X), len = %d",
                        meta, meta, len));
                javaType = Types.BINARY;
                value = binary;
                length = len;
                break;
            }
        default:
            logger.error(String.format(
                    "!! Don't know how to handle column type=%d meta=%d (%04X)",
                    type, meta, meta));
            javaType = Types.OTHER;
            value = null;
            length = 0;
        }

        return value;
    }

    public final boolean isNull()
    {
        return fNull;
    }

    public final int getJavaType()
    {
        return javaType;
    }

    public final Serializable getValue()
    {
        return value;
    }

    public final int getLength()
    {
        return length;
    }
}
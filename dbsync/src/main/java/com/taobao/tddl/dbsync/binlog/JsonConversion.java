package com.taobao.tddl.dbsync.binlog;

import static com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer.appendNumber2;
import static com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer.appendNumber4;
import static com.taobao.tddl.dbsync.binlog.event.RowsLogBuffer.usecondsToStr;

/**
 * 处理下MySQL json二进制转化为可读的字符串
 * 
 * @author agapple 2016年6月30日 上午11:26:17
 * @since 1.0.22
 */
public class JsonConversion {

    // JSON TYPE
    public static final int  JSONB_TYPE_SMALL_OBJECT = 0x0;
    public static final int  JSONB_TYPE_LARGE_OBJECT = 0x1;
    public static final int  JSONB_TYPE_SMALL_ARRAY  = 0x2;
    public static final int  JSONB_TYPE_LARGE_ARRAY  = 0x3;
    public static final int  JSONB_TYPE_LITERAL      = 0x4;
    public static final int  JSONB_TYPE_INT16        = 0x5;
    public static final int  JSONB_TYPE_UINT16       = 0x6;
    public static final int  JSONB_TYPE_INT32        = 0x7;
    public static final int  JSONB_TYPE_UINT32       = 0x8;
    public static final int  JSONB_TYPE_INT64        = 0x9;
    public static final int  JSONB_TYPE_UINT64       = 0xA;
    public static final int  JSONB_TYPE_DOUBLE       = 0xB;
    public static final int  JSONB_TYPE_STRING       = 0xC;
    public static final int  JSONB_TYPE_OPAQUE       = 0xF;
    public static final char JSONB_NULL_LITERAL      = '\0';
    public static final char JSONB_TRUE_LITERAL      = '\1';
    public static final char JSONB_FALSE_LITERAL     = '\2';

    /*
     * The size of offset or size fields in the small and the large storage
     * format for JSON objects and JSON arrays.
     */
    public static final int  SMALL_OFFSET_SIZE       = 2;
    public static final int  LARGE_OFFSET_SIZE       = 4;

    /*
     * The size of key entries for objects when using the small storage format
     * or the large storage format. In the small format it is 4 bytes (2 bytes
     * for key length and 2 bytes for key offset). In the large format it is 6
     * (2 bytes for length, 4 bytes for offset).
     */
    public static final int  KEY_ENTRY_SIZE_SMALL    = (2 + SMALL_OFFSET_SIZE);
    public static final int  KEY_ENTRY_SIZE_LARGE    = (2 + LARGE_OFFSET_SIZE);

    /*
     * The size of value entries for objects or arrays. When using the small
     * storage format, the entry size is 3 (1 byte for type, 2 bytes for
     * offset). When using the large storage format, it is 5 (1 byte for type, 4
     * bytes for offset).
     */
    public static final int  VALUE_ENTRY_SIZE_SMALL  = (1 + SMALL_OFFSET_SIZE);
    public static final int  VALUE_ENTRY_SIZE_LARGE  = (1 + LARGE_OFFSET_SIZE);

    public static Json_Value parse_value(int type, LogBuffer buffer, long len, String charsetName) {
        buffer = buffer.duplicate(buffer.position(), (int) len);
        switch (type) {
            case JSONB_TYPE_SMALL_OBJECT:
                return parse_array_or_object(Json_enum_type.OBJECT, buffer, len, false, charsetName);
            case JSONB_TYPE_LARGE_OBJECT:
                return parse_array_or_object(Json_enum_type.OBJECT, buffer, len, true, charsetName);
            case JSONB_TYPE_SMALL_ARRAY:
                return parse_array_or_object(Json_enum_type.ARRAY, buffer, len, false, charsetName);
            case JSONB_TYPE_LARGE_ARRAY:
                return parse_array_or_object(Json_enum_type.ARRAY, buffer, len, true, charsetName);
            default:
                return parse_scalar(type, buffer, len, charsetName);
        }
    }

    private static Json_Value parse_array_or_object(Json_enum_type type, LogBuffer buffer, long len, boolean large,
                                                    String charsetName) {
        long offset_size = large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
        if (len < 2 * offset_size) {
            throw new IllegalArgumentException("illegal json data");
        }
        long element_count = read_offset_or_size(buffer, large);
        long bytes = read_offset_or_size(buffer, large);

        if (bytes > len) {
            throw new IllegalArgumentException("illegal json data");
        }
        long header_size = 2 * offset_size;
        if (type == Json_enum_type.OBJECT) {
            header_size += element_count * (large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL);
        }

        header_size += element_count * (large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL);
        if (header_size > bytes) {
            throw new IllegalArgumentException("illegal json data");
        }
        return new Json_Value(type, buffer.rewind(), element_count, bytes, large);
    }

    private static long read_offset_or_size(LogBuffer buffer, boolean large) {
        return large ? buffer.getUint32() : buffer.getUint16();
    }

    private static Json_Value parse_scalar(int type, LogBuffer buffer, long len, String charsetName) {
        switch (type) {
            case JSONB_TYPE_LITERAL:
                /* purecov: inspected */
                int data = buffer.getUint8();
                switch (data) {
                    case JSONB_NULL_LITERAL:
                        return new Json_Value(Json_enum_type.LITERAL_NULL);
                    case JSONB_TRUE_LITERAL:
                        return new Json_Value(Json_enum_type.LITERAL_TRUE);
                    case JSONB_FALSE_LITERAL:
                        return new Json_Value(Json_enum_type.LITERAL_FALSE);
                    default:
                        throw new IllegalArgumentException("illegal json data");
                }
            case JSONB_TYPE_INT16:
                return new Json_Value(Json_enum_type.INT, buffer.getInt16());
            case JSONB_TYPE_INT32:
                return new Json_Value(Json_enum_type.INT, buffer.getInt32());
            case JSONB_TYPE_INT64:
                return new Json_Value(Json_enum_type.INT, buffer.getLong64());
            case JSONB_TYPE_UINT16:
                return new Json_Value(Json_enum_type.UINT, buffer.getUint16());
            case JSONB_TYPE_UINT32:
                return new Json_Value(Json_enum_type.UINT, buffer.getUint32());
            case JSONB_TYPE_UINT64:
                return new Json_Value(Json_enum_type.UINT, buffer.getUlong64());
            case JSONB_TYPE_DOUBLE:
                return new Json_Value(Json_enum_type.DOUBLE, Double.valueOf(buffer.getDouble64()));
            case JSONB_TYPE_STRING:
                int max_bytes = (int) Math.min(len, 5);
                long tlen = 0;
                long str_len = 0;
                long n = 0;
                byte[] datas = buffer.getData(max_bytes);
                for (int i = 0; i < max_bytes; i++) {
                    // Get the next 7 bits of the length.
                    tlen |= (datas[i] & 0x7f) << (7 * i);
                    if ((datas[i] & 0x80) == 0) {
                        // The length shouldn't exceed 32 bits.
                        if (tlen > 4294967296L) {
                            throw new IllegalArgumentException("illegal json data");
                        }

                        // This was the last byte. Return successfully.
                        n = i + 1;
                        str_len = tlen;
                        break;
                    }
                }

                if (len < n + str_len) {
                    throw new IllegalArgumentException("illegal json data");
                }
                return new Json_Value(Json_enum_type.STRING, buffer.rewind()
                    .forward((int) n)
                    .getFixString((int) str_len, charsetName));
            case JSONB_TYPE_OPAQUE:
                /*
                 * There should always be at least one byte, which tells the
                 * field type of the opaque value.
                 */
                // The type is encoded as a uint8 that maps to an
                // enum_field_types.
                int type_byte = buffer.getUint8();
                int position = buffer.position();
                // Then there's the length of the value.
                int q_max_bytes = (int) Math.min(len, 5);
                long q_tlen = 0;
                long q_str_len = 0;
                long q_n = 0;
                byte[] q_datas = buffer.getData(q_max_bytes);
                for (int i = 0; i < q_max_bytes; i++) {
                    // Get the next 7 bits of the length.
                    q_tlen |= (q_datas[i] & 0x7f) << (7 * i);
                    if ((q_datas[i] & 0x80) == 0) {
                        // The length shouldn't exceed 32 bits.
                        if (q_tlen > 4294967296L) {
                            throw new IllegalArgumentException("illegal json data");
                        }

                        // This was the last byte. Return successfully.
                        q_n = i + 1;
                        q_str_len = q_tlen;
                        break;
                    }
                }

                if (q_str_len == 0 || len < q_n + q_str_len) {
                    throw new IllegalArgumentException("illegal json data");
                }
                return new Json_Value(type_byte, buffer.position(position).forward((int) q_n), q_str_len);
            default:
                throw new IllegalArgumentException("illegal json data");
        }
    }

    public static class Json_Value {

        Json_enum_type m_type;
        int            m_field_type;
        LogBuffer      m_data;
        long           m_element_count;
        long           m_length;
        String         m_string_value;
        Number         m_int_value;
        double         m_double_value;
        boolean        m_large;

        public Json_Value(Json_enum_type t){
            this.m_type = t;
        }

        public Json_Value(Json_enum_type t, Number val){
            this.m_type = t;
            if (t == Json_enum_type.DOUBLE) {
                this.m_double_value = val.doubleValue();
            } else {
                this.m_int_value = val;
            }
        }

        public Json_Value(Json_enum_type t, String value){
            this.m_type = t;
            this.m_string_value = value;
        }

        public Json_Value(int field_type, LogBuffer data, long bytes){
            this.m_type = Json_enum_type.OPAQUE; // 不确定类型
            this.m_field_type = field_type;
            this.m_data = data;
            this.m_length = bytes;
        }

        public Json_Value(Json_enum_type t, LogBuffer data, long element_count, long bytes, boolean large){
            this.m_type = t;
            this.m_data = data;
            this.m_element_count = element_count;
            this.m_length = bytes;
            this.m_large = large;
        }

        public String key(int i, String charsetName) {
            m_data.rewind();
            int offset_size = m_large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
            int key_entry_size = m_large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
            int entry_offset = 2 * offset_size + key_entry_size * i;
            // The offset of the key is the first part of the key
            // entry.
            m_data.forward(entry_offset);
            long key_offset = read_offset_or_size(m_data, m_large);
            // The length of the key is the second part of the
            // entry, always two
            // bytes.
            long key_length = m_data.getUint16();
            return m_data.rewind().forward((int) key_offset).getFixString((int) key_length, charsetName);
        }

        public Json_Value element(int i, String charsetName) {
            m_data.rewind();
            int offset_size = m_large ? LARGE_OFFSET_SIZE : SMALL_OFFSET_SIZE;
            int key_entry_size = m_large ? KEY_ENTRY_SIZE_LARGE : KEY_ENTRY_SIZE_SMALL;
            int value_entry_size = m_large ? VALUE_ENTRY_SIZE_LARGE : VALUE_ENTRY_SIZE_SMALL;
            int first_entry_offset = 2 * offset_size;
            if (m_type == Json_enum_type.OBJECT) {
                first_entry_offset += m_element_count * key_entry_size;
            }
            int entry_offset = first_entry_offset + value_entry_size * i;
            int type = m_data.forward(entry_offset).getUint8();
            if (type == JSONB_TYPE_INT16 || type == JSONB_TYPE_UINT16 || type == JSONB_TYPE_LITERAL
                || (m_large && (type == JSONB_TYPE_INT32 || type == JSONB_TYPE_UINT32))) {
                return parse_scalar(type, m_data, value_entry_size - 1, charsetName);
            }
            int value_offset = (int) read_offset_or_size(m_data, m_large);
            return parse_value(type, m_data.rewind().forward(value_offset), (int) m_length - value_offset, charsetName);
        }

        public StringBuilder toJsonString(StringBuilder buf, String charsetName) {
            switch (m_type) {
                case OBJECT:
                    buf.append("{");
                    for (int i = 0; i < m_element_count; ++i) {
                        if (i > 0) {
                            buf.append(", ");
                        }
                        buf.append('"').append(key(i, charsetName)).append('"');
                        buf.append(": ");
                        element(i, charsetName).toJsonString(buf, charsetName);
                    }
                    buf.append("}");
                    break;
                case ARRAY:
                    buf.append("[");
                    for (int i = 0; i < m_element_count; ++i) {
                        if (i > 0) {
                            buf.append(", ");
                        }
                        element(i, charsetName).toJsonString(buf, charsetName);
                    }
                    buf.append("]");
                    break;
                case DOUBLE:
                    buf.append(Double.valueOf(m_double_value).toString());
                    break;
                case INT:
                    buf.append(m_int_value.toString());
                    break;
                case UINT:
                    buf.append(m_int_value.toString());
                    break;
                case LITERAL_FALSE:
                    buf.append("false");
                    break;
                case LITERAL_TRUE:
                    buf.append("true");
                    break;
                case LITERAL_NULL:
                    buf.append("null");
                    break;
                case OPAQUE:
                    String text = null;
                    if (m_field_type == LogEvent.MYSQL_TYPE_NEWDECIMAL) {
                        int precision = m_data.getInt8();
                        int scale = m_data.getInt8();
                        text = m_data.getDecimal(precision, scale).toPlainString();
                        buf.append(text);
                    } else if (m_field_type == LogEvent.MYSQL_TYPE_TIME) {
                        long packed_value = m_data.getLong64();
                        if (packed_value == 0) {
                            text = "00:00:00";
                        } else {
                            long ultime = Math.abs(packed_value);
                            long intpart = ultime >> 24;
                            int frac = (int) (ultime % (1L << 24));
                            // text = String.format("%s%02d:%02d:%02d",
                            // packed_value >= 0 ? "" : "-",
                            // (int) ((intpart >> 12) % (1 << 10)),
                            // (int) ((intpart >> 6) % (1 << 6)),
                            // (int) (intpart % (1 << 6)));
                            // text = text + "." + usecondsToStr(frac, 6);
                            StringBuilder builder = new StringBuilder(17);
                            if (packed_value < 0) {
                                builder.append('-');
                            }

                            int d = (int) ((intpart >> 12) % (1 << 10));
                            if (d > 100) {
                                builder.append(String.valueOf(d));
                            } else {
                                appendNumber2(builder, d);
                            }
                            builder.append(':');
                            appendNumber2(builder, (int) ((intpart >> 6) % (1 << 6)));
                            builder.append(':');
                            appendNumber2(builder, (int) (intpart % (1 << 6)));

                            builder.append('.').append(usecondsToStr(frac, 6));
                            text = builder.toString();
                        }
                        buf.append('"').append(text).append('"');
                    } else if (m_field_type == LogEvent.MYSQL_TYPE_DATE || m_field_type == LogEvent.MYSQL_TYPE_DATETIME
                               || m_field_type == LogEvent.MYSQL_TYPE_TIMESTAMP) {
                        long packed_value = m_data.getLong64();
                        if (packed_value == 0) {
                            text = "0000-00-00 00:00:00";
                        } else {
                            // 构造TimeStamp只处理到秒
                            long ultime = Math.abs(packed_value);
                            long intpart = ultime >> 24;
                            int frac = (int) (ultime % (1L << 24));
                            long ymd = intpart >> 17;
                            long ym = ymd >> 5;
                            long hms = intpart % (1 << 17);
                            // text =
                            // String.format("%04d-%02d-%02d %02d:%02d:%02d",
                            // (int) (ym / 13),
                            // (int) (ym % 13),
                            // (int) (ymd % (1 << 5)),
                            // (int) (hms >> 12),
                            // (int) ((hms >> 6) % (1 << 6)),
                            // (int) (hms % (1 << 6)));
                            StringBuilder builder = new StringBuilder(26);
                            appendNumber4(builder, (int) (ym / 13));
                            builder.append('-');
                            appendNumber2(builder, (int) (ym % 13));
                            builder.append('-');
                            appendNumber2(builder, (int) (ymd % (1 << 5)));
                            builder.append(' ');
                            appendNumber2(builder, (int) (hms >> 12));
                            builder.append(':');
                            appendNumber2(builder, (int) ((hms >> 6) % (1 << 6)));
                            builder.append(':');
                            appendNumber2(builder, (int) (hms % (1 << 6)));
                            builder.append('.').append(usecondsToStr(frac, 6));
                            text = builder.toString();
                        }
                        buf.append('"').append(text).append('"');
                    } else {
                        text = m_data.getFixString((int) m_length, charsetName);
                        buf.append('"').append(escapse(text)).append('"');
                    }

                    break;
                case STRING:
                    buf.append('"').append(escapse(m_string_value)).append('"');
                    break;
                case ERROR:
                    throw new IllegalArgumentException("illegal json data");
            }

            return buf;
        }
    }

    private static StringBuilder escapse(String data) {
        StringBuilder sb = new StringBuilder(data.length());
        int endIndex = data.length();
        for (int i = 0; i < endIndex; ++i) {
            char c = data.charAt(i);
            if (c == '"') {
                sb.append('\\');
            } else if (c == '\\') {
                sb.append("\\");
            }
            sb.append(c);
        }
        return sb;
    }

    public static enum Json_enum_type {
        OBJECT, ARRAY, STRING, INT, UINT, DOUBLE, LITERAL_NULL, LITERAL_TRUE, LITERAL_FALSE, OPAQUE, ERROR
    }

}

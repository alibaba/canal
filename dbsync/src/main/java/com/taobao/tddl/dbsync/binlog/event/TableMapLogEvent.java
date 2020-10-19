package com.taobao.tddl.dbsync.binlog.event;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * In row-based mode, every row operation event is preceded by a
 * Table_map_log_event which maps a table definition to a number. The table
 * definition consists of database name, table name, and column definitions. The
 * Post-Header has the following components:
 * <table>
 * <caption>Post-Header for Table_map_log_event</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>table_id</td>
 * <td>6 bytes unsigned integer</td>
 * <td>The number that identifies the table.</td>
 * </tr>
 * <tr>
 * <td>flags</td>
 * <td>2 byte bitfield</td>
 * <td>Reserved for future use; currently always 0.</td>
 * </tr>
 * </table>
 * The Body has the following components:
 * <table>
 * <caption>Body for Table_map_log_event</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>database_name</td>
 * <td>one byte string length, followed by null-terminated string</td>
 * <td>The name of the database in which the table resides. The name is
 * represented as a one byte unsigned integer representing the number of bytes
 * in the name, followed by length bytes containing the database name, followed
 * by a terminating 0 byte. (Note the redundancy in the representation of the
 * length.)</td>
 * </tr>
 * <tr>
 * <td>table_name</td>
 * <td>one byte string length, followed by null-terminated string</td>
 * <td>The name of the table, encoded the same way as the database name above.</td>
 * </tr>
 * <tr>
 * <td>column_count</td>
 * <td>packed_integer "Packed Integer"</td>
 * <td>The number of columns in the table, represented as a packed
 * variable-length integer.</td>
 * </tr>
 * <tr>
 * <td>column_type</td>
 * <td>List of column_count 1 byte enumeration values</td>
 * <td>The type of each column in the table, listed from left to right. Each
 * byte is mapped to a column type according to the enumeration type
 * enum_field_types defined in mysql_com.h. The mapping of types to numbers is
 * listed in the table Table_table_map_log_event_column_types "below" (along
 * with description of the associated metadata field).</td>
 * </tr>
 * <tr>
 * <td>metadata_length</td>
 * <td>packed_integer "Packed Integer"</td>
 * <td>The length of the following metadata block</td>
 * </tr>
 * <tr>
 * <td>metadata</td>
 * <td>list of metadata for each column</td>
 * <td>For each column from left to right, a chunk of data who's length and
 * semantics depends on the type of the column. The length and semantics for the
 * metadata for each column are listed in the table
 * Table_table_map_log_event_column_types "below".</td>
 * </tr>
 * <tr>
 * <td>null_bits</td>
 * <td>column_count bits, rounded up to nearest byte</td>
 * <td>For each column, a bit indicating whether data in the column can be NULL
 * or not. The number of bytes needed for this is int((column_count+7)/8). The
 * flag for the first column from the left is in the least-significant bit of
 * the first byte, the second is in the second least significant bit of the
 * first byte, the ninth is in the least significant bit of the second byte, and
 * so on.</td>
 * </tr>
 * <tr>
 * <td>optional metadata fields</td>
 * <td>optional metadata fields are stored in Type, Length, Value(TLV) format.
 * Type takes 1 byte. Length is a packed integer value. Values takes Length
 * bytes.</td>
 * <td>There are some optional metadata defined. They are listed in the table
 * 
 * @ref Table_table_map_event_optional_metadata. Optional metadata fields follow
 * null_bits. Whether binlogging an optional metadata is decided by the server.
 * The order is not defined, so they can be binlogged in any order. </td>
 * </tr>
 * </table>
 * The table below lists all column types, along with the numerical identifier
 * for it and the size and interpretation of meta-data used to describe the
 * type.
 * <table>
 * <caption>Table_map_log_event column types: numerical identifier and
 * metadata</caption>
 * <tr>
 * <th>Name</th>
 * <th>Identifier</th>
 * <th>Size of metadata in bytes</th>
 * <th>Description of metadata</th>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_DECIMAL</td>
 * <td>0</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_TINY</td>
 * <td>1</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_SHORT</td>
 * <td>2</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_LONG</td>
 * <td>3</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_FLOAT</td>
 * <td>4</td>
 * <td>1 byte</td>
 * <td>1 byte unsigned integer, representing the "pack_length", which is equal
 * to sizeof(float) on the server from which the event originates.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_DOUBLE</td>
 * <td>5</td>
 * <td>1 byte</td>
 * <td>1 byte unsigned integer, representing the "pack_length", which is equal
 * to sizeof(double) on the server from which the event originates.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_NULL</td>
 * <td>6</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_TIMESTAMP</td>
 * <td>7</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_LONGLONG</td>
 * <td>8</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_INT24</td>
 * <td>9</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_DATE</td>
 * <td>10</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_TIME</td>
 * <td>11</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_DATETIME</td>
 * <td>12</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_YEAR</td>
 * <td>13</td>
 * <td>0</td>
 * <td>No column metadata.</td>
 * </tr>
 * <tr>
 * <td><i>MYSQL_TYPE_NEWDATE</i></td>
 * <td><i>14</i></td>
 * <td>&ndash;</td>
 * <td><i>This enumeration value is only used internally and cannot exist in a
 * binlog.</i></td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_VARCHAR</td>
 * <td>15</td>
 * <td>2 bytes</td>
 * <td>2 byte unsigned integer representing the maximum length of the string.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_BIT</td>
 * <td>16</td>
 * <td>2 bytes</td>
 * <td>A 1 byte unsigned int representing the length in bits of the bitfield (0
 * to 64), followed by a 1 byte unsigned int representing the number of bytes
 * occupied by the bitfield. The number of bytes is either int((length+7)/8) or
 * int(length/8).</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_NEWDECIMAL</td>
 * <td>246</td>
 * <td>2 bytes</td>
 * <td>A 1 byte unsigned int representing the precision, followed by a 1 byte
 * unsigned int representing the number of decimals.</td>
 * </tr>
 * <tr>
 * <td><i>MYSQL_TYPE_ENUM</i></td>
 * <td><i>247</i></td>
 * <td>&ndash;</td>
 * <td><i>This enumeration value is only used internally and cannot exist in a
 * binlog.</i></td>
 * </tr>
 * <tr>
 * <td><i>MYSQL_TYPE_SET</i></td>
 * <td><i>248</i></td>
 * <td>&ndash;</td>
 * <td><i>This enumeration value is only used internally and cannot exist in a
 * binlog.</i></td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_TINY_BLOB</td>
 * <td>249</td>
 * <td>&ndash;</td>
 * <td><i>This enumeration value is only used internally and cannot exist in a
 * binlog.</i></td>
 * </tr>
 * <tr>
 * <td><i>MYSQL_TYPE_MEDIUM_BLOB</i></td>
 * <td><i>250</i></td>
 * <td>&ndash;</td>
 * <td><i>This enumeration value is only used internally and cannot exist in a
 * binlog.</i></td>
 * </tr>
 * <tr>
 * <td><i>MYSQL_TYPE_LONG_BLOB</i></td>
 * <td><i>251</i></td>
 * <td>&ndash;</td>
 * <td><i>This enumeration value is only used internally and cannot exist in a
 * binlog.</i></td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_BLOB</td>
 * <td>252</td>
 * <td>1 byte</td>
 * <td>The pack length, i.e., the number of bytes needed to represent the length
 * of the blob: 1, 2, 3, or 4.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_VAR_STRING</td>
 * <td>253</td>
 * <td>2 bytes</td>
 * <td>This is used to store both strings and enumeration values. The first byte
 * is a enumeration value storing the <i>real type</i>, which may be either
 * MYSQL_TYPE_VAR_STRING or MYSQL_TYPE_ENUM. The second byte is a 1 byte
 * unsigned integer representing the field size, i.e., the number of bytes
 * needed to store the length of the string.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_STRING</td>
 * <td>254</td>
 * <td>2 bytes</td>
 * <td>The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253). The second
 * byte is the field size, i.e., the number of bytes in the representation of
 * size of the string: 3 or 4.</td>
 * </tr>
 * <tr>
 * <td>MYSQL_TYPE_GEOMETRY</td>
 * <td>255</td>
 * <td>1 byte</td>
 * <td>The pack length, i.e., the number of bytes needed to represent the length
 * of the geometry: 1, 2, 3, or 4.</td>
 * </tr>
 * </table>
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class TableMapLogEvent extends LogEvent {

    /**
     * Fixed data part:
     * <ul>
     * <li>6 bytes. The table ID.</li>
     * <li>2 bytes. Reserved for future use.</li>
     * </ul>
     * <p>
     * Variable data part:
     * <ul>
     * <li>1 byte. The length of the database name.</li>
     * <li>Variable-sized. The database name (null-terminated).</li>
     * <li>1 byte. The length of the table name.</li>
     * <li>Variable-sized. The table name (null-terminated).</li>
     * <li>Packed integer. The number of columns in the table.</li>
     * <li>Variable-sized. An array of column types, one byte per column.</li>
     * <li>Packed integer. The length of the metadata block.</li>
     * <li>Variable-sized. The metadata block; see log_event.h for contents and
     * format.</li>
     * <li>Variable-sized. Bit-field indicating whether each column can be NULL,
     * one bit per column. For this field, the amount of storage required for N
     * columns is INT((N+7)/8) bytes.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    protected String dbname;
    protected String tblname;

    /**
     * Holding mysql column information.
     */
    public static final class ColumnInfo {

        public int          type;
        public int          meta;
        public String       name;
        public boolean      unsigned;
        public boolean      pk;
        public List<String> set_enum_values;
        public int          charset;        // 可以通过CharsetUtil进行转化
        public int          geoType;
        public boolean      nullable;

        @Override
        public String toString() {
            return "ColumnInfo [type=" + type + ", meta=" + meta + ", name=" + name + ", unsigned=" + unsigned
                   + ", pk=" + pk + ", set_enum_values=" + set_enum_values + ", charset=" + charset + ", geoType="
                   + geoType + ", nullable=" + nullable + "]";
        }
    }

    protected final int          columnCnt;
    protected final ColumnInfo[] columnInfo;                     // buffer
                                                                  // for
                                                                  // field
                                                                  // metadata

    protected final long         tableId;
    protected BitSet             nullBits;

    /** TM = "Table Map" */
    public static final int      TM_MAPID_OFFSET         = 0;
    public static final int      TM_FLAGS_OFFSET         = 6;

    // UNSIGNED flag of numeric columns
    public static final int      SIGNEDNESS              = 1;
    // Default character set of string columns
    public static final int      DEFAULT_CHARSET         = 2;
    // Character set of string columns
    public static final int      COLUMN_CHARSET          = 3;
    public static final int      COLUMN_NAME             = 4;
    // String value of SET columns
    public static final int      SET_STR_VALUE           = 5;
    // String value of ENUM columns
    public static final int      ENUM_STR_VALUE          = 6;
    // Real type of geometry columns
    public static final int      GEOMETRY_TYPE           = 7;
    // Primary key without prefix
    public static final int      SIMPLE_PRIMARY_KEY      = 8;
    // Primary key with prefix
    public static final int      PRIMARY_KEY_WITH_PREFIX = 9;

    private int                  default_charset;
    private boolean              existOptionalMetaData   = false;

    private static final class Pair {

        public int col_index;
        public int col_charset;
    }

    /**
     * Constructor used by slave to read the event from the binary log.
     */
    public TableMapLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        final int postHeaderLen = descriptionEvent.postHeaderLen[header.type - 1];
        /* Read the post-header */
        buffer.position(commonHeaderLen + TM_MAPID_OFFSET);
        if (postHeaderLen == 6) {
            /*
             * Master is of an intermediate source tree before 5.1.4. Id is 4
             * bytes
             */
            tableId = buffer.getUint32();
        } else {
            // DBUG_ASSERT(post_header_len == TABLE_MAP_HEADER_LEN);
            tableId = buffer.getUlong48();
        }
        // flags = buffer.getUint16();

        /* Read the variable part of the event */
        buffer.position(commonHeaderLen + postHeaderLen);
        dbname = buffer.getString();
        buffer.forward(1); /* termination null */
        // fixed issue #2714
        tblname = buffer.getString();
        buffer.forward(1); /* termination null */

        // Read column information from buffer
        columnCnt = (int) buffer.getPackedLong();
        columnInfo = new ColumnInfo[columnCnt];
        for (int i = 0; i < columnCnt; i++) {
            ColumnInfo info = new ColumnInfo();
            info.type = buffer.getUint8();
            columnInfo[i] = info;
        }

        if (buffer.position() < buffer.limit()) {
            final int fieldSize = (int) buffer.getPackedLong();
            decodeFields(buffer, fieldSize);
            nullBits = buffer.getBitmap(columnCnt);

            for (int i = 0; i < columnCnt; i++) {
                if (nullBits.get(i)) {
                    columnInfo[i].nullable = true;
                }
            }
            /*
             * After null_bits field, there are some new fields for extra
             * metadata.
             */
            existOptionalMetaData = false;
            List<TableMapLogEvent.Pair> defaultCharsetPairs = null;
            List<Integer> columnCharsets = null;
            while (buffer.hasRemaining()) {
                // optional metadata fields
                int type = buffer.getUint8();
                int len = (int) buffer.getPackedLong();

                switch (type) {
                    case SIGNEDNESS:
                        parse_signedness(buffer, len);
                        break;
                    case DEFAULT_CHARSET:
                        defaultCharsetPairs = parse_default_charset(buffer, len);
                        break;
                    case COLUMN_CHARSET:
                        columnCharsets = parse_column_charset(buffer, len);
                        break;
                    case COLUMN_NAME:
                        // set @@global.binlog_row_metadata='FULL'
                        // 主要是补充列名相关信息
                        existOptionalMetaData = true;
                        parse_column_name(buffer, len);
                        break;
                    case SET_STR_VALUE:
                        parse_set_str_value(buffer, len, true);
                        break;
                    case ENUM_STR_VALUE:
                        parse_set_str_value(buffer, len, false);
                        break;
                    case GEOMETRY_TYPE:
                        parse_geometry_type(buffer, len);
                        break;
                    case SIMPLE_PRIMARY_KEY:
                        parse_simple_pk(buffer, len);
                        break;
                    case PRIMARY_KEY_WITH_PREFIX:
                        parse_pk_with_prefix(buffer, len);
                        break;
                    default:
                        throw new IllegalArgumentException("unknow type : " + type);
                }
            }

            if (existOptionalMetaData) {
                int index = 0;
                int char_col_index = 0;
                for (int i = 0; i < columnCnt; i++) {
                    int cs = -1;
                    int type = getRealType(columnInfo[i].type, columnInfo[i].meta);
                    if (is_character_type(type)) {
                        if (defaultCharsetPairs != null && !defaultCharsetPairs.isEmpty()) {
                            if (index < defaultCharsetPairs.size()
                                && char_col_index == defaultCharsetPairs.get(index).col_index) {
                                cs = defaultCharsetPairs.get(index).col_charset;
                                index++;
                            } else {
                                cs = default_charset;
                            }

                            char_col_index++;
                        } else if (columnCharsets != null) {
                            cs = columnCharsets.get(index);
                            index++;
                        }

                        columnInfo[i].charset = cs;
                    }
                }
            }
        }

        // for (int i = 0; i < columnCnt; i++) {
        // System.out.println(columnInfo[i]);
        // }
    }

    /**
     * Decode field metadata by column types.
     * 
     * @see mysql-5.1.60/sql/rpl_utility.h
     */
    private final void decodeFields(LogBuffer buffer, final int len) {
        final int limit = buffer.limit();

        buffer.limit(len + buffer.position());
        for (int i = 0; i < columnCnt; i++) {
            ColumnInfo info = columnInfo[i];

            switch (info.type) {
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_GEOMETRY:
                case MYSQL_TYPE_JSON:
                    /*
                     * These types store a single byte.
                     */
                    info.meta = buffer.getUint8();
                    break;
                case MYSQL_TYPE_SET:
                case MYSQL_TYPE_ENUM:
                    /*
                     * log_event.h : MYSQL_TYPE_SET & MYSQL_TYPE_ENUM : This
                     * enumeration value is only used internally and cannot
                     * exist in a binlog.
                     */
                    logger.warn("This enumeration value is only used internally "
                                + "and cannot exist in a binlog: type=" + info.type);
                    break;
                case MYSQL_TYPE_STRING: {
                    /*
                     * log_event.h : The first byte is always
                     * MYSQL_TYPE_VAR_STRING (i.e., 253). The second byte is the
                     * field size, i.e., the number of bytes in the
                     * representation of size of the string: 3 or 4.
                     */
                    int x = (buffer.getUint8() << 8); // real_type
                    x += buffer.getUint8(); // pack or field length
                    info.meta = x;
                    break;
                }
                case MYSQL_TYPE_BIT:
                    info.meta = buffer.getUint16();
                    break;
                case MYSQL_TYPE_VARCHAR:
                    /*
                     * These types store two bytes.
                     */
                    info.meta = buffer.getUint16();
                    break;
                case MYSQL_TYPE_NEWDECIMAL: {
                    int x = buffer.getUint8() << 8; // precision
                    x += buffer.getUint8(); // decimals
                    info.meta = x;
                    break;
                }
                case MYSQL_TYPE_TIME2:
                case MYSQL_TYPE_DATETIME2:
                case MYSQL_TYPE_TIMESTAMP2: {
                    info.meta = buffer.getUint8();
                    break;
                }
                default:
                    info.meta = 0;
                    break;
            }
        }
        buffer.limit(limit);
    }

    private void parse_signedness(LogBuffer buffer, int length) {
        // stores the signedness flags extracted from field
        List<Boolean> datas = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            int ut = buffer.getUint8();
            for (int c = 0x80; c != 0; c >>= 1) {
                datas.add((ut & c) > 0);
            }
        }

        int index = 0;
        for (int i = 0; i < columnCnt; i++) {
            if (is_numeric_type(columnInfo[i].type)) {
                columnInfo[i].unsigned = datas.get(index);
                index++;
            }
        }
    }

    private List<TableMapLogEvent.Pair> parse_default_charset(LogBuffer buffer, int length) {
        // stores collation numbers extracted from field.
        int limit = buffer.position() + length;
        this.default_charset = (int) buffer.getPackedLong();
        List<TableMapLogEvent.Pair> datas = new ArrayList<>();
        while (buffer.hasRemaining() && buffer.position() < limit) {
            int col_index = (int) buffer.getPackedLong();
            int col_charset = (int) buffer.getPackedLong();

            Pair pair = new Pair();
            pair.col_index = col_index;
            pair.col_charset = col_charset;
            datas.add(pair);
        }

        return datas;
    }

    private List<Integer> parse_column_charset(LogBuffer buffer, int length) {
        // stores collation numbers extracted from field.
        int limit = buffer.position() + length;
        List<Integer> datas = new ArrayList<>();
        while (buffer.hasRemaining() && buffer.position() < limit) {
            int col_charset = (int) buffer.getPackedLong();
            datas.add(col_charset);
        }

        return datas;
    }

    private void parse_column_name(LogBuffer buffer, int length) {
        // stores column names extracted from field
        int limit = buffer.position() + length;
        int index = 0;
        while (buffer.hasRemaining() && buffer.position() < limit) {
            int len = (int) buffer.getPackedLong();
            columnInfo[index++].name = buffer.getFixString(len);
        }
    }

    private void parse_set_str_value(LogBuffer buffer, int length, boolean set) {
        // stores SET/ENUM column's string values extracted from
        // field. Each SET/ENUM column's string values are stored
        // into a string separate vector. All of them are stored
        // in 'vec'.
        int limit = buffer.position() + length;
        List<List<String>> datas = new ArrayList<>();
        while (buffer.hasRemaining() && buffer.position() < limit) {
            int count = (int) buffer.getPackedLong();
            List<String> data = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int len1 = (int) buffer.getPackedLong();
                data.add(buffer.getFixString(len1));
            }

            datas.add(data);
        }

        int index = 0;
        for (int i = 0; i < columnCnt; i++) {
            if (set && getRealType(columnInfo[i].type, columnInfo[i].meta) == LogEvent.MYSQL_TYPE_SET) {
                columnInfo[i].set_enum_values = datas.get(index);
                index++;
            }

            if (!set && getRealType(columnInfo[i].type, columnInfo[i].meta) == LogEvent.MYSQL_TYPE_ENUM) {
                columnInfo[i].set_enum_values = datas.get(index);
                index++;
            }
        }
    }

    private void parse_geometry_type(LogBuffer buffer, int length) {
        // stores geometry column's types extracted from field.
        int limit = buffer.position() + length;

        List<Integer> datas = new ArrayList<>();
        while (buffer.hasRemaining() && buffer.position() < limit) {
            int col_type = (int) buffer.getPackedLong();
            datas.add(col_type);
        }

        int index = 0;
        for (int i = 0; i < columnCnt; i++) {
            if (columnInfo[i].type == LogEvent.MYSQL_TYPE_GEOMETRY) {
                columnInfo[i].geoType = datas.get(index);
                index++;
            }
        }
    }

    private void parse_simple_pk(LogBuffer buffer, int length) {
        // stores primary key's column information extracted from
        // field. Each column has an index and a prefix which are
        // stored as a unit_pair. prefix is always 0 for
        // SIMPLE_PRIMARY_KEY field.

        int limit = buffer.position() + length;
        while (buffer.hasRemaining() && buffer.position() < limit) {
            int col_index = (int) buffer.getPackedLong();
            columnInfo[col_index].pk = true;
        }
    }

    private void parse_pk_with_prefix(LogBuffer buffer, int length) {
        // stores primary key's column information extracted from
        // field. Each column has an index and a prefix which are
        // stored as a unit_pair.
        int limit = buffer.position() + length;
        while (buffer.hasRemaining() && buffer.position() < limit) {
            int col_index = (int) buffer.getPackedLong();
            // prefix length, 比如 char(32)
            @SuppressWarnings("unused")
            int col_prefix = (int) buffer.getPackedLong();
            columnInfo[col_index].pk = true;
        }
    }

    private boolean is_numeric_type(int type) {
        switch (type) {
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
            case MYSQL_TYPE_NEWDECIMAL:
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
                return true;
            default:
                return false;
        }
    }

    private boolean is_character_type(int type) {
        switch (type) {
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_BLOB:
                return true;
            default:
                return false;
        }
    }

    private int getRealType(int type, int meta) {
        if (type == LogEvent.MYSQL_TYPE_STRING) {
            if (meta >= 256) {
                int byte0 = meta >> 8;
                if ((byte0 & 0x30) != 0x30) {
                    /* a long CHAR() field: see #37426 */
                    type = byte0 | 0x30;
                } else {
                    switch (byte0) {
                        case LogEvent.MYSQL_TYPE_SET:
                        case LogEvent.MYSQL_TYPE_ENUM:
                        case LogEvent.MYSQL_TYPE_STRING:
                            type = byte0;
                    }
                }
            }
        }

        return type;
    }

    public final String getDbName() {
        return dbname;
    }

    public final String getTableName() {
        return tblname;
    }

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public String getTblname() {
        return tblname;
    }

    public void setTblname(String tblname) {
        this.tblname = tblname;
    }

    public final int getColumnCnt() {
        return columnCnt;
    }

    public final ColumnInfo[] getColumnInfo() {
        return columnInfo;
    }

    public final long getTableId() {
        return tableId;
    }

    public boolean isExistOptionalMetaData() {
        return existOptionalMetaData;
    }

    public void setExistOptionalMetaData(boolean existOptional) {
        this.existOptionalMetaData = existOptional;
    }

}

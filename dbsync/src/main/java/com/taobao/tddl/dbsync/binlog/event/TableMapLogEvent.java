package com.taobao.tddl.dbsync.binlog.event;

import java.util.BitSet;

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
 * 
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
    protected final String dbname;
    protected final String tblname;

    /**
     * Holding mysql column information.
     */
    public static final class ColumnInfo {

        public int type;
        public int meta;
    }

    protected final int          columnCnt;
    protected final ColumnInfo[] columnInfo;         // buffer for field
                                                      // metadata

    protected final long         tableId;
    protected BitSet             nullBits;

    /** TM = "Table Map" */
    public static final int      TM_MAPID_OFFSET = 0;
    public static final int      TM_FLAGS_OFFSET = 6;

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
        }
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

    public final String getDbName() {
        return dbname;
    }

    public final String getTableName() {
        return tblname;
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
}

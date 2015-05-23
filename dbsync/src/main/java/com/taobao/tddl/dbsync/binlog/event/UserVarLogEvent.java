package com.taobao.tddl.dbsync.binlog.event;

import java.io.IOException;
import java.io.Serializable;

import com.taobao.tddl.dbsync.binlog.CharsetConversion;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * User_var_log_event. Every time a query uses the value of a user variable, a
 * User_var_log_event is written before the Query_log_event, to set the user
 * variable.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class UserVarLogEvent extends LogEvent {

    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>4 bytes. the size of the user variable name.</li>
     * <li>The user variable name.</li>
     * <li>1 byte. Non-zero if the variable value is the SQL NULL value, 0
     * otherwise. If this byte is 0, the following parts exist in the event.</li>
     * <li>1 byte. The user variable type. The value corresponds to elements of
     * enum Item_result defined in include/mysql_com.h.</li>
     * <li>4 bytes. The number of the character set for the user variable
     * (needed for a string variable). The character set number is really a
     * collation number that indicates a character set/collation pair.</li>
     * <li>4 bytes. The size of the user variable value (corresponds to member
     * val_len of class Item_string).</li>
     * <li>Variable-sized. For a string variable, this is the string. For a
     * float or integer variable, this is its value in 8 bytes.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    private final String       name;
    private final Serializable value;
    private final int          type;
    private final int          charsetNumber;
    private final boolean      isNull;

    /**
     * The following is for user defined functions
     * 
     * @see mysql-5.1.60//include/mysql_com.h
     */
    public static final int    STRING_RESULT          = 0;
    public static final int    REAL_RESULT            = 1;
    public static final int    INT_RESULT             = 2;
    public static final int    ROW_RESULT             = 3;
    public static final int    DECIMAL_RESULT         = 4;

    /* User_var event data */
    public static final int    UV_VAL_LEN_SIZE        = 4;
    public static final int    UV_VAL_IS_NULL         = 1;
    public static final int    UV_VAL_TYPE_SIZE       = 1;
    public static final int    UV_NAME_LEN_SIZE       = 4;
    public static final int    UV_CHARSET_NUMBER_SIZE = 4;

    public UserVarLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent)
                                                                                                          throws IOException{
        super(header);

        /* The Post-Header is empty. The Variable Data part begins immediately. */
        buffer.position(descriptionEvent.commonHeaderLen + descriptionEvent.postHeaderLen[USER_VAR_EVENT - 1]);
        final int nameLen = (int) buffer.getUint32();
        name = buffer.getFixString(nameLen); // UV_NAME_LEN_SIZE
        isNull = (0 != buffer.getInt8());

        if (isNull) {
            type = STRING_RESULT;
            charsetNumber = 63; /* binary */
            value = null;
        } else {
            type = buffer.getInt8(); // UV_VAL_IS_NULL
            charsetNumber = (int) buffer.getUint32(); // buf + UV_VAL_TYPE_SIZE
            final int valueLen = (int) buffer.getUint32(); // buf +
                                                           // UV_CHARSET_NUMBER_SIZE
            final int limit = buffer.limit(); /* for restore */
            buffer.limit(buffer.position() + valueLen);

            /* @see User_var_log_event::print */
            switch (type) {
                case REAL_RESULT:
                    value = Double.valueOf(buffer.getDouble64()); // float8get
                    break;
                case INT_RESULT:
                    if (valueLen == 8) value = Long.valueOf(buffer.getLong64()); // !uint8korr
                    else if (valueLen == 4) value = Long.valueOf(buffer.getUint32());
                    else throw new IOException("Error INT_RESULT length: " + valueLen);
                    break;
                case DECIMAL_RESULT:
                    final int precision = buffer.getInt8();
                    final int scale = buffer.getInt8();
                    value = buffer.getDecimal(precision, scale); // bin2decimal
                    break;
                case STRING_RESULT:
                    String charsetName = CharsetConversion.getJavaCharset(charsetNumber);
                    value = buffer.getFixString(valueLen, charsetName);
                    break;
                case ROW_RESULT:
                    // this seems to be banned in MySQL altogether
                    throw new IOException("ROW_RESULT is unsupported");
                default:
                    value = null;
                    break;
            }
            buffer.limit(limit);
        }
    }

    public final String getQuery() {
        if (value == null) {
            return "SET @" + name + " := NULL";
        } else if (type == STRING_RESULT) {
            // TODO: do escaping !?
            return "SET @" + name + " := \'" + value + '\'';
        } else {
            return "SET @" + name + " := " + String.valueOf(value);
        }
    }
}

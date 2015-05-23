package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * An Intvar_log_event will be created just before a Query_log_event, if the
 * query uses one of the variables LAST_INSERT_ID or INSERT_ID. Each
 * Intvar_log_event holds the value of one of these variables. Binary Format The
 * Post-Header for this event type is empty. The Body has two components:
 * <table>
 * <caption>Body for Intvar_log_event</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>type</td>
 * <td>1 byte enumeration</td>
 * <td>One byte identifying the type of variable stored. Currently, two
 * identifiers are supported: LAST_INSERT_ID_EVENT==1 and INSERT_ID_EVENT==2.</td>
 * </tr>
 * <tr>
 * <td>value</td>
 * <td>8 byte unsigned integer</td>
 * <td>The value of the variable.</td>
 * </tr>
 * </table>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class IntvarLogEvent extends LogEvent {

    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>1 byte. A value indicating the variable type: LAST_INSERT_ID_EVENT =
     * 1 or INSERT_ID_EVENT = 2.</li>
     * <li>8 bytes. An unsigned integer indicating the value to be used for the
     * LAST_INSERT_ID() invocation or AUTO_INCREMENT column.</li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    private final long      value;
    private final int       type;

    /* Intvar event data */
    public static final int I_TYPE_OFFSET        = 0;
    public static final int I_VAL_OFFSET         = 1;

    // enum Int_event_type
    public static final int INVALID_INT_EVENT    = 0;
    public static final int LAST_INSERT_ID_EVENT = 1;
    public static final int INSERT_ID_EVENT      = 2;

    public IntvarLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        /* The Post-Header is empty. The Varible Data part begins immediately. */
        buffer.position(descriptionEvent.commonHeaderLen + descriptionEvent.postHeaderLen[INTVAR_EVENT - 1]
                        + I_TYPE_OFFSET);
        type = buffer.getInt8(); // I_TYPE_OFFSET
        value = buffer.getLong64(); // !uint8korr(buf + I_VAL_OFFSET);
    }

    public final int getType() {
        return type;
    }

    public final long getValue() {
        return value;
    }

    public final String getQuery() {
        return "SET INSERT_ID = " + value;
    }
}

package com.taobao.tddl.dbsync.binlog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.dbsync.binlog.event.LogHeader;

/**
 * Binary log event definitions. This includes generic code common to all types
 * of log events, as well as specific code for each type of log event. - All
 * numbers, whether they are 16-, 24-, 32-, or 64-bit numbers, are stored in
 * little endian, i.e., the least significant byte first, unless otherwise
 * specified. representation of unsigned integers, called Packed Integer. A
 * Packed Integer has the capacity of storing up to 8-byte integers, while small
 * integers still can use 1, 3, or 4 bytes. The value of the first byte
 * determines how to read the number, according to the following table:
 * <table>
 * <caption>Format of Packed Integer</caption>
 * <tr>
 * <th>First byte</th>
 * <th>Format</th>
 * </tr>
 * <tr>
 * <td>0-250</td>
 * <td>The first byte is the number (in the range 0-250), and no more bytes are
 * used.</td>
 * </tr>
 * <tr>
 * <td>252</td>
 * <td>Two more bytes are used. The number is in the range 251-0xffff.</td>
 * </tr>
 * <tr>
 * <td>253</td>
 * <td>Three more bytes are used. The number is in the range 0xffff-0xffffff.</td>
 * </tr>
 * <tr>
 * <td>254</td>
 * <td>Eight more bytes are used. The number is in the range
 * 0xffffff-0xffffffffffffffff.</td>
 * </tr>
 * </table>
 * - Strings are stored in various formats. The format of each string is
 * documented separately.
 * 
 * @see mysql-5.1.60/sql/log_event.h
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public abstract class LogEvent {

    /*
     * 3 is MySQL 4.x; 4 is MySQL 5.0.0. Compared to version 3, version 4 has: -
     * a different Start_log_event, which includes info about the binary log
     * (sizes of headers); this info is included for better compatibility if the
     * master's MySQL version is different from the slave's. - all events have a
     * unique ID (the triplet (server_id, timestamp at server start, other) to
     * be sure an event is not executed more than once in a multimaster setup,
     * example: M1 / \ v v M2 M3 \ / v v S if a query is run on M1, it will
     * arrive twice on S, so we need that S remembers the last unique ID it has
     * processed, to compare and know if the event should be skipped or not.
     * Example of ID: we already have the server id (4 bytes), plus:
     * timestamp_when_the_master_started (4 bytes), a counter (a sequence number
     * which increments every time we write an event to the binlog) (3 bytes).
     * Q: how do we handle when the counter is overflowed and restarts from 0 ?
     * - Query and Load (Create or Execute) events may have a more precise
     * timestamp (with microseconds), number of matched/affected/warnings rows
     * and fields of session variables: SQL_MODE, FOREIGN_KEY_CHECKS,
     * UNIQUE_CHECKS, SQL_AUTO_IS_NULL, the collations and charsets, the
     * PASSWORD() version (old/new/...).
     */
    public static final int    BINLOG_VERSION                           = 4;

    /* Default 5.0 server version */
    public static final String SERVER_VERSION                           = "5.0";

    /**
     * Event header offsets; these point to places inside the fixed header.
     */
    public static final int    EVENT_TYPE_OFFSET                        = 4;
    public static final int    SERVER_ID_OFFSET                         = 5;
    public static final int    EVENT_LEN_OFFSET                         = 9;
    public static final int    LOG_POS_OFFSET                           = 13;
    public static final int    FLAGS_OFFSET                             = 17;

    /* event-specific post-header sizes */
    // where 3.23, 4.x and 5.0 agree
    public static final int    QUERY_HEADER_MINIMAL_LEN                 = (4 + 4 + 1 + 2);
    // where 5.0 differs: 2 for len of N-bytes vars.
    public static final int    QUERY_HEADER_LEN                         = (QUERY_HEADER_MINIMAL_LEN + 2);

    /* Enumeration type for the different types of log events. */
    public static final int    UNKNOWN_EVENT                            = 0;
    public static final int    START_EVENT_V3                           = 1;
    public static final int    QUERY_EVENT                              = 2;
    public static final int    STOP_EVENT                               = 3;
    public static final int    ROTATE_EVENT                             = 4;
    public static final int    INTVAR_EVENT                             = 5;
    public static final int    LOAD_EVENT                               = 6;
    public static final int    SLAVE_EVENT                              = 7;
    public static final int    CREATE_FILE_EVENT                        = 8;
    public static final int    APPEND_BLOCK_EVENT                       = 9;
    public static final int    EXEC_LOAD_EVENT                          = 10;
    public static final int    DELETE_FILE_EVENT                        = 11;

    /**
     * NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer sql_ex,
     * allowing multibyte TERMINATED BY etc; both types share the same class
     * (Load_log_event)
     */
    public static final int    NEW_LOAD_EVENT                           = 12;
    public static final int    RAND_EVENT                               = 13;
    public static final int    USER_VAR_EVENT                           = 14;
    public static final int    FORMAT_DESCRIPTION_EVENT                 = 15;
    public static final int    XID_EVENT                                = 16;
    public static final int    BEGIN_LOAD_QUERY_EVENT                   = 17;
    public static final int    EXECUTE_LOAD_QUERY_EVENT                 = 18;

    public static final int    TABLE_MAP_EVENT                          = 19;

    /**
     * These event numbers were used for 5.1.0 to 5.1.15 and are therefore
     * obsolete.
     */
    public static final int    PRE_GA_WRITE_ROWS_EVENT                  = 20;
    public static final int    PRE_GA_UPDATE_ROWS_EVENT                 = 21;
    public static final int    PRE_GA_DELETE_ROWS_EVENT                 = 22;

    /**
     * These event numbers are used from 5.1.16 and forward
     */
    public static final int    WRITE_ROWS_EVENT_V1                      = 23;
    public static final int    UPDATE_ROWS_EVENT_V1                     = 24;
    public static final int    DELETE_ROWS_EVENT_V1                     = 25;

    /**
     * Something out of the ordinary happened on the master
     */
    public static final int    INCIDENT_EVENT                           = 26;

    /**
     * Heartbeat event to be send by master at its idle time to ensure master's
     * online status to slave
     */
    public static final int    HEARTBEAT_LOG_EVENT                      = 27;

    /**
     * In some situations, it is necessary to send over ignorable data to the
     * slave: data that a slave can handle in case there is code for handling
     * it, but which can be ignored if it is not recognized.
     */
    public static final int    IGNORABLE_LOG_EVENT                      = 28;
    public static final int    ROWS_QUERY_LOG_EVENT                     = 29;

    /** Version 2 of the Row events */
    public static final int    WRITE_ROWS_EVENT                         = 30;
    public static final int    UPDATE_ROWS_EVENT                        = 31;
    public static final int    DELETE_ROWS_EVENT                        = 32;

    public static final int    GTID_LOG_EVENT                           = 33;
    public static final int    ANONYMOUS_GTID_LOG_EVENT                 = 34;

    public static final int    PREVIOUS_GTIDS_LOG_EVENT                 = 35;

    /* MySQL 5.7 events */
    public static final int    TRANSACTION_CONTEXT_EVENT                = 36;

    public static final int    VIEW_CHANGE_EVENT                        = 37;

    /* Prepared XA transaction terminal event similar to Xid */
    public static final int    XA_PREPARE_LOG_EVENT                     = 38;

    /**
     * Extension of UPDATE_ROWS_EVENT, allowing partial values according to
     * binlog_row_value_options.
     */
    public static final int    PARTIAL_UPDATE_ROWS_EVENT                = 39;

    // mariaDb 5.5.34
    /* New MySQL/Sun events are to be added right above this comment */
    public static final int    MYSQL_EVENTS_END                         = 49;

    public static final int    MARIA_EVENTS_BEGIN                       = 160;
    /* New Maria event numbers start from here */
    public static final int    ANNOTATE_ROWS_EVENT                      = 160;
    /*
     * Binlog checkpoint event. Used for XA crash recovery on the master, not
     * used in replication. A binlog checkpoint event specifies a binlog file
     * such that XA crash recovery can start from that file - and it is
     * guaranteed to find all XIDs that are prepared in storage engines but not
     * yet committed.
     */
    public static final int    BINLOG_CHECKPOINT_EVENT                  = 161;
    /*
     * Gtid event. For global transaction ID, used to start a new event group,
     * instead of the old BEGIN query event, and also to mark stand-alone
     * events.
     */
    public static final int    GTID_EVENT                               = 162;
    /*
     * Gtid list event. Logged at the start of every binlog, to record the
     * current replication state. This consists of the last GTID seen for each
     * replication domain.
     */
    public static final int    GTID_LIST_EVENT                          = 163;

    public static final int    START_ENCRYPTION_EVENT                   = 164;

    /** end marker */
    public static final int    ENUM_END_EVENT                           = 165;

    /**
     * 1 byte length, 1 byte format Length is total length in bytes, including 2
     * byte header Length values 0 and 1 are currently invalid and reserved.
     */
    public static final int    EXTRA_ROW_INFO_LEN_OFFSET                = 0;
    public static final int    EXTRA_ROW_INFO_FORMAT_OFFSET             = 1;
    public static final int    EXTRA_ROW_INFO_HDR_BYTES                 = 2;
    public static final int    EXTRA_ROW_INFO_MAX_PAYLOAD               = (255 - EXTRA_ROW_INFO_HDR_BYTES);

    // Events are without checksum though its generator
    public static final int    BINLOG_CHECKSUM_ALG_OFF                  = 0;
    // is checksum-capable New Master (NM).
    // CRC32 of zlib algorithm.
    public static final int    BINLOG_CHECKSUM_ALG_CRC32                = 1;
    // the cut line: valid alg range is [1, 0x7f].
    public static final int    BINLOG_CHECKSUM_ALG_ENUM_END             = 2;
    // special value to tag undetermined yet checksum
    public static final int    BINLOG_CHECKSUM_ALG_UNDEF                = 255;
    // or events from checksum-unaware servers

    public static final int    CHECKSUM_CRC32_SIGNATURE_LEN             = 4;
    public static final int    BINLOG_CHECKSUM_ALG_DESC_LEN             = 1;
    /**
     * defined statically while there is just one alg implemented
     */
    public static final int    BINLOG_CHECKSUM_LEN                      = CHECKSUM_CRC32_SIGNATURE_LEN;

    /* MySQL or old MariaDB slave with no announced capability. */
    public static final int    MARIA_SLAVE_CAPABILITY_UNKNOWN           = 0;

    /* MariaDB >= 5.3, which understands ANNOTATE_ROWS_EVENT. */
    public static final int    MARIA_SLAVE_CAPABILITY_ANNOTATE          = 1;
    /*
     * MariaDB >= 5.5. This version has the capability to tolerate events
     * omitted from the binlog stream without breaking replication (MySQL slaves
     * fail because they mis-compute the offsets into the master's binlog).
     */
    public static final int    MARIA_SLAVE_CAPABILITY_TOLERATE_HOLES    = 2;
    /* MariaDB >= 10.0, which knows about binlog_checkpoint_log_event. */
    public static final int    MARIA_SLAVE_CAPABILITY_BINLOG_CHECKPOINT = 3;
    /* MariaDB >= 10.0.1, which knows about global transaction id events. */
    public static final int    MARIA_SLAVE_CAPABILITY_GTID              = 4;

    /* Our capability. */
    public static final int    MARIA_SLAVE_CAPABILITY_MINE              = MARIA_SLAVE_CAPABILITY_GTID;

    /**
     * For an event, 'e', carrying a type code, that a slave, 's', does not
     * recognize, 's' will check 'e' for LOG_EVENT_IGNORABLE_F, and if the flag
     * is set, then 'e' is ignored. Otherwise, 's' acknowledges that it has
     * found an unknown event in the relay log.
     */
    public static final int    LOG_EVENT_IGNORABLE_F                    = 0x80;

    /** enum_field_types */
    public static final int    MYSQL_TYPE_DECIMAL                       = 0;
    public static final int    MYSQL_TYPE_TINY                          = 1;
    public static final int    MYSQL_TYPE_SHORT                         = 2;
    public static final int    MYSQL_TYPE_LONG                          = 3;
    public static final int    MYSQL_TYPE_FLOAT                         = 4;
    public static final int    MYSQL_TYPE_DOUBLE                        = 5;
    public static final int    MYSQL_TYPE_NULL                          = 6;
    public static final int    MYSQL_TYPE_TIMESTAMP                     = 7;
    public static final int    MYSQL_TYPE_LONGLONG                      = 8;
    public static final int    MYSQL_TYPE_INT24                         = 9;
    public static final int    MYSQL_TYPE_DATE                          = 10;
    public static final int    MYSQL_TYPE_TIME                          = 11;
    public static final int    MYSQL_TYPE_DATETIME                      = 12;
    public static final int    MYSQL_TYPE_YEAR                          = 13;
    public static final int    MYSQL_TYPE_NEWDATE                       = 14;
    public static final int    MYSQL_TYPE_VARCHAR                       = 15;
    public static final int    MYSQL_TYPE_BIT                           = 16;
    public static final int    MYSQL_TYPE_TIMESTAMP2                    = 17;
    public static final int    MYSQL_TYPE_DATETIME2                     = 18;
    public static final int    MYSQL_TYPE_TIME2                         = 19;
    public static final int    MYSQL_TYPE_JSON                          = 245;
    public static final int    MYSQL_TYPE_NEWDECIMAL                    = 246;
    public static final int    MYSQL_TYPE_ENUM                          = 247;
    public static final int    MYSQL_TYPE_SET                           = 248;
    public static final int    MYSQL_TYPE_TINY_BLOB                     = 249;
    public static final int    MYSQL_TYPE_MEDIUM_BLOB                   = 250;
    public static final int    MYSQL_TYPE_LONG_BLOB                     = 251;
    public static final int    MYSQL_TYPE_BLOB                          = 252;
    public static final int    MYSQL_TYPE_VAR_STRING                    = 253;
    public static final int    MYSQL_TYPE_STRING                        = 254;
    public static final int    MYSQL_TYPE_GEOMETRY                      = 255;

    public static String getTypeName(final int type) {
        switch (type) {
            case START_EVENT_V3:
                return "Start_v3";
            case STOP_EVENT:
                return "Stop";
            case QUERY_EVENT:
                return "Query";
            case ROTATE_EVENT:
                return "Rotate";
            case INTVAR_EVENT:
                return "Intvar";
            case LOAD_EVENT:
                return "Load";
            case NEW_LOAD_EVENT:
                return "New_load";
            case SLAVE_EVENT:
                return "Slave";
            case CREATE_FILE_EVENT:
                return "Create_file";
            case APPEND_BLOCK_EVENT:
                return "Append_block";
            case DELETE_FILE_EVENT:
                return "Delete_file";
            case EXEC_LOAD_EVENT:
                return "Exec_load";
            case RAND_EVENT:
                return "RAND";
            case XID_EVENT:
                return "Xid";
            case USER_VAR_EVENT:
                return "User var";
            case FORMAT_DESCRIPTION_EVENT:
                return "Format_desc";
            case TABLE_MAP_EVENT:
                return "Table_map";
            case PRE_GA_WRITE_ROWS_EVENT:
                return "Write_rows_event_old";
            case PRE_GA_UPDATE_ROWS_EVENT:
                return "Update_rows_event_old";
            case PRE_GA_DELETE_ROWS_EVENT:
                return "Delete_rows_event_old";
            case WRITE_ROWS_EVENT_V1:
                return "Write_rows_v1";
            case UPDATE_ROWS_EVENT_V1:
                return "Update_rows_v1";
            case DELETE_ROWS_EVENT_V1:
                return "Delete_rows_v1";
            case BEGIN_LOAD_QUERY_EVENT:
                return "Begin_load_query";
            case EXECUTE_LOAD_QUERY_EVENT:
                return "Execute_load_query";
            case INCIDENT_EVENT:
                return "Incident";
            case HEARTBEAT_LOG_EVENT:
                return "Heartbeat";
            case IGNORABLE_LOG_EVENT:
                return "Ignorable";
            case ROWS_QUERY_LOG_EVENT:
                return "Rows_query";
            case WRITE_ROWS_EVENT:
                return "Write_rows";
            case UPDATE_ROWS_EVENT:
                return "Update_rows";
            case DELETE_ROWS_EVENT:
                return "Delete_rows";
            case GTID_LOG_EVENT:
                return "Gtid";
            case ANONYMOUS_GTID_LOG_EVENT:
                return "Anonymous_Gtid";
            case PREVIOUS_GTIDS_LOG_EVENT:
                return "Previous_gtids";
            case PARTIAL_UPDATE_ROWS_EVENT:
                return "Update_rows_partial";
            default:
                return "Unknown"; /* impossible */
        }
    }

    protected static final Log logger = LogFactory.getLog(LogEvent.class);

    protected final LogHeader  header;

    /**
     * mysql半同步semi标识
     * 
     * <pre>
     * 0不需要semi ack 给mysql
     * 1需要semi ack给mysql
     * </pre>
     */
    protected int              semival;

    public int getSemival() {
        return semival;
    }

    public void setSemival(int semival) {
        this.semival = semival;
    }

    protected LogEvent(LogHeader header){
        this.header = header;
    }

    /**
     * Return event header.
     */
    public final LogHeader getHeader() {
        return header;
    }

    /**
     * The total size of this event, in bytes. In other words, this is the sum
     * of the sizes of Common-Header, Post-Header, and Body.
     */
    public final int getEventLen() {
        return header.getEventLen();
    }

    /**
     * Server ID of the server that created the event.
     */
    public final long getServerId() {
        return header.getServerId();
    }

    /**
     * The position of the next event in the master binary log, in bytes from
     * the beginning of the file. In a binlog that is not a relay log, this is
     * just the position of the next event, in bytes from the beginning of the
     * file. In a relay log, this is the position of the next event in the
     * master's binlog.
     */
    public final long getLogPos() {
        return header.getLogPos();
    }

    /**
     * The time when the query started, in seconds since 1970.
     */
    public final long getWhen() {
        return header.getWhen();
    }

}

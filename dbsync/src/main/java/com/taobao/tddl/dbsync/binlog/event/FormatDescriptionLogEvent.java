package com.taobao.tddl.dbsync.binlog.event;

import java.io.IOException;

import com.taobao.tddl.dbsync.binlog.LogBuffer;

/**
 * For binlog version 4. This event is saved by threads which read it, as they
 * need it for future use (to decode the ordinary events).
 * 
 * @see mysql-5.1.60/sql/log_event.cc - Format_description_log_event
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class FormatDescriptionLogEvent extends StartLogEventV3 {

    /**
     * The number of types we handle in Format_description_log_event
     * (UNKNOWN_EVENT is not to be handled, it does not exist in binlogs, it
     * does not have a format).
     */
    public static final int   LOG_EVENT_TYPES                     = (ENUM_END_EVENT - 1);

    public static final int   ST_COMMON_HEADER_LEN_OFFSET         = (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN + 4);

    public static final int   OLD_HEADER_LEN                      = 13;
    public static final int   LOG_EVENT_HEADER_LEN                = 19;
    public static final int   LOG_EVENT_MINIMAL_HEADER_LEN        = 19;

    /* event-specific post-header sizes */
    public static final int   STOP_HEADER_LEN                     = 0;
    public static final int   LOAD_HEADER_LEN                     = (4 + 4 + 4 + 1 + 1 + 4);
    public static final int   SLAVE_HEADER_LEN                    = 0;
    public static final int   START_V3_HEADER_LEN                 = (2 + ST_SERVER_VER_LEN + 4);
    public static final int   ROTATE_HEADER_LEN                   = 8;                                                       // this
    // is
    // FROZEN
    // (the
    // Rotate
    // post-header
    // is
    // frozen)
    public static final int   INTVAR_HEADER_LEN                   = 0;
    public static final int   CREATE_FILE_HEADER_LEN              = 4;
    public static final int   APPEND_BLOCK_HEADER_LEN             = 4;
    public static final int   EXEC_LOAD_HEADER_LEN                = 4;
    public static final int   DELETE_FILE_HEADER_LEN              = 4;
    public static final int   NEW_LOAD_HEADER_LEN                 = LOAD_HEADER_LEN;
    public static final int   RAND_HEADER_LEN                     = 0;
    public static final int   USER_VAR_HEADER_LEN                 = 0;
    public static final int   FORMAT_DESCRIPTION_HEADER_LEN       = (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES);
    public static final int   XID_HEADER_LEN                      = 0;
    public static final int   BEGIN_LOAD_QUERY_HEADER_LEN         = APPEND_BLOCK_HEADER_LEN;
    public static final int   ROWS_HEADER_LEN_V1                  = 8;
    public static final int   TABLE_MAP_HEADER_LEN                = 8;
    public static final int   EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = (4 + 4 + 4 + 1);
    public static final int   EXECUTE_LOAD_QUERY_HEADER_LEN       = (QUERY_HEADER_LEN + EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN);
    public static final int   INCIDENT_HEADER_LEN                 = 2;
    public static final int   HEARTBEAT_HEADER_LEN                = 0;
    public static final int   IGNORABLE_HEADER_LEN                = 0;
    public static final int   ROWS_HEADER_LEN_V2                  = 10;
    public static final int   TRANSACTION_CONTEXT_HEADER_LEN      = 18;
    public static final int   VIEW_CHANGE_HEADER_LEN              = 52;
    public static final int   XA_PREPARE_HEADER_LEN               = 0;

    public static final int   ANNOTATE_ROWS_HEADER_LEN            = 0;
    public static final int   BINLOG_CHECKPOINT_HEADER_LEN        = 4;
    public static final int   GTID_HEADER_LEN                     = 19;
    public static final int   GTID_LIST_HEADER_LEN                = 4;
    public static final int   START_ENCRYPTION_HEADER_LEN         = 0;

    public static final int   POST_HEADER_LENGTH                  = 11;

    public static final int   BINLOG_CHECKSUM_ALG_DESC_LEN        = 1;
    public static final int[] checksumVersionSplit                = { 5, 6, 1 };
    public static final long  checksumVersionProduct              = (checksumVersionSplit[0] * 256 + checksumVersionSplit[1])
                                                                    * 256 + checksumVersionSplit[2];
    /**
     * The size of the fixed header which _all_ events have (for binlogs written
     * by this version, this is equal to LOG_EVENT_HEADER_LEN), except
     * FORMAT_DESCRIPTION_EVENT and ROTATE_EVENT (those have a header of size
     * LOG_EVENT_MINIMAL_HEADER_LEN).
     */
    protected final int       commonHeaderLen;
    protected int             numberOfEventTypes;

    /** The list of post-headers' lengthes */
    protected final short[]   postHeaderLen;
    protected int[]           serverVersionSplit                  = new int[3];

    public FormatDescriptionLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent)
                                                                                                                    throws IOException{
        /* Start_log_event_v3 */
        super(header, buffer, descriptionEvent);

        buffer.position(LOG_EVENT_MINIMAL_HEADER_LEN + ST_COMMON_HEADER_LEN_OFFSET);
        commonHeaderLen = buffer.getUint8();
        if (commonHeaderLen < OLD_HEADER_LEN) /* sanity check */
        {
            throw new IOException("Format Description event header length is too short");
        }

        numberOfEventTypes = buffer.limit() - (LOG_EVENT_MINIMAL_HEADER_LEN + ST_COMMON_HEADER_LEN_OFFSET + 1);

        // buffer.position(LOG_EVENT_MINIMAL_HEADER_LEN
        // + ST_COMMON_HEADER_LEN_OFFSET + 1);
        postHeaderLen = new short[numberOfEventTypes];
        for (int i = 0; i < numberOfEventTypes; i++) {
            postHeaderLen[i] = (short) buffer.getUint8();
        }

        calcServerVersionSplit();
        long calc = getVersionProduct();
        if (calc >= checksumVersionProduct) {
            /*
             * the last bytes are the checksum alg desc and value (or value's
             * room)
             */
            numberOfEventTypes -= BINLOG_CHECKSUM_ALG_DESC_LEN;
        }

        if (logger.isInfoEnabled()) logger.info("common_header_len= " + commonHeaderLen + ", number_of_event_types= "
                                                + numberOfEventTypes);
    }

    /** MySQL 5.0 format descriptions. */
    public static final FormatDescriptionLogEvent FORMAT_DESCRIPTION_EVENT_5_x   = new FormatDescriptionLogEvent(4);

    /** MySQL 4.0.x (x>=2) format descriptions. */
    public static final FormatDescriptionLogEvent FORMAT_DESCRIPTION_EVENT_4_0_x = new FormatDescriptionLogEvent(3);

    /** MySQL 3.23 format descriptions. */
    public static final FormatDescriptionLogEvent FORMAT_DESCRIPTION_EVENT_3_23  = new FormatDescriptionLogEvent(1);

    public static FormatDescriptionLogEvent getFormatDescription(final int binlogVersion) throws IOException {
        /* identify binlog format */
        switch (binlogVersion) {
            case 4: /* MySQL 5.0 */
                return FORMAT_DESCRIPTION_EVENT_5_x;
            case 3:
                return FORMAT_DESCRIPTION_EVENT_4_0_x;
            case 1:
                return FORMAT_DESCRIPTION_EVENT_3_23;
            default:
                throw new IOException("Unknown binlog version: " + binlogVersion);
        }
    }

    public FormatDescriptionLogEvent(final int binlogVersion, int binlogChecksum){
        this(binlogVersion);
        this.header.checksumAlg = binlogChecksum;
    }

    public FormatDescriptionLogEvent(final int binlogVersion){
        this.binlogVersion = binlogVersion;

        postHeaderLen = new short[ENUM_END_EVENT];
        /* identify binlog format */
        switch (binlogVersion) {
            case 4: /* MySQL 5.0 */
                serverVersion = SERVER_VERSION;
                commonHeaderLen = LOG_EVENT_HEADER_LEN;
                numberOfEventTypes = LOG_EVENT_TYPES;

                /*
                 * Note: all event types must explicitly fill in their lengths
                 * here.
                 */
                postHeaderLen[START_EVENT_V3 - 1] = START_V3_HEADER_LEN;
                postHeaderLen[QUERY_EVENT - 1] = QUERY_HEADER_LEN;
                postHeaderLen[STOP_EVENT - 1] = STOP_HEADER_LEN;
                postHeaderLen[ROTATE_EVENT - 1] = ROTATE_HEADER_LEN;
                postHeaderLen[INTVAR_EVENT - 1] = INTVAR_HEADER_LEN;
                postHeaderLen[LOAD_EVENT - 1] = LOAD_HEADER_LEN;
                postHeaderLen[SLAVE_EVENT - 1] = SLAVE_HEADER_LEN;
                postHeaderLen[CREATE_FILE_EVENT - 1] = CREATE_FILE_HEADER_LEN;
                postHeaderLen[APPEND_BLOCK_EVENT - 1] = APPEND_BLOCK_HEADER_LEN;
                postHeaderLen[EXEC_LOAD_EVENT - 1] = EXEC_LOAD_HEADER_LEN;
                postHeaderLen[DELETE_FILE_EVENT - 1] = DELETE_FILE_HEADER_LEN;
                postHeaderLen[NEW_LOAD_EVENT - 1] = NEW_LOAD_HEADER_LEN;
                postHeaderLen[RAND_EVENT - 1] = RAND_HEADER_LEN;
                postHeaderLen[USER_VAR_EVENT - 1] = USER_VAR_HEADER_LEN;
                postHeaderLen[FORMAT_DESCRIPTION_EVENT - 1] = FORMAT_DESCRIPTION_HEADER_LEN;
                postHeaderLen[XID_EVENT - 1] = XID_HEADER_LEN;
                postHeaderLen[BEGIN_LOAD_QUERY_EVENT - 1] = BEGIN_LOAD_QUERY_HEADER_LEN;
                postHeaderLen[EXECUTE_LOAD_QUERY_EVENT - 1] = EXECUTE_LOAD_QUERY_HEADER_LEN;
                postHeaderLen[TABLE_MAP_EVENT - 1] = TABLE_MAP_HEADER_LEN;
                postHeaderLen[WRITE_ROWS_EVENT_V1 - 1] = ROWS_HEADER_LEN_V1;
                postHeaderLen[UPDATE_ROWS_EVENT_V1 - 1] = ROWS_HEADER_LEN_V1;
                postHeaderLen[DELETE_ROWS_EVENT_V1 - 1] = ROWS_HEADER_LEN_V1;
                /*
                 * We here have the possibility to simulate a master of before
                 * we changed the table map id to be stored in 6 bytes: when it
                 * was stored in 4 bytes (=> post_header_len was 6). This is
                 * used to test backward compatibility. This code can be removed
                 * after a few months (today is Dec 21st 2005), when we know
                 * that the 4-byte masters are not deployed anymore (check with
                 * Tomas Ulin first!), and the accompanying test
                 * (rpl_row_4_bytes) too.
                 */
                postHeaderLen[HEARTBEAT_LOG_EVENT - 1] = 0;
                postHeaderLen[IGNORABLE_LOG_EVENT - 1] = IGNORABLE_HEADER_LEN;
                postHeaderLen[ROWS_QUERY_LOG_EVENT - 1] = IGNORABLE_HEADER_LEN;
                postHeaderLen[WRITE_ROWS_EVENT - 1] = ROWS_HEADER_LEN_V2;
                postHeaderLen[UPDATE_ROWS_EVENT - 1] = ROWS_HEADER_LEN_V2;
                postHeaderLen[DELETE_ROWS_EVENT - 1] = ROWS_HEADER_LEN_V2;
                postHeaderLen[GTID_LOG_EVENT - 1] = POST_HEADER_LENGTH;
                postHeaderLen[ANONYMOUS_GTID_LOG_EVENT - 1] = POST_HEADER_LENGTH;
                postHeaderLen[PREVIOUS_GTIDS_LOG_EVENT - 1] = IGNORABLE_HEADER_LEN;

                postHeaderLen[TRANSACTION_CONTEXT_EVENT - 1] = TRANSACTION_CONTEXT_HEADER_LEN;
                postHeaderLen[VIEW_CHANGE_EVENT - 1] = VIEW_CHANGE_HEADER_LEN;
                postHeaderLen[XA_PREPARE_LOG_EVENT - 1] = XA_PREPARE_HEADER_LEN;
                postHeaderLen[PARTIAL_UPDATE_ROWS_EVENT - 1] = ROWS_HEADER_LEN_V2;

                // mariadb 10
                postHeaderLen[ANNOTATE_ROWS_EVENT - 1] = ANNOTATE_ROWS_HEADER_LEN;
                postHeaderLen[BINLOG_CHECKPOINT_EVENT - 1] = BINLOG_CHECKPOINT_HEADER_LEN;
                postHeaderLen[GTID_EVENT - 1] = GTID_HEADER_LEN;
                postHeaderLen[GTID_LIST_EVENT - 1] = GTID_LIST_HEADER_LEN;
                postHeaderLen[START_ENCRYPTION_EVENT - 1] = START_ENCRYPTION_HEADER_LEN;
                break;

            case 3: /* 4.0.x x>=2 */
                /*
                 * We build an artificial (i.e. not sent by the master) event,
                 * which describes what those old master versions send.
                 */
                serverVersion = "4.0";
                commonHeaderLen = LOG_EVENT_MINIMAL_HEADER_LEN;

                /*
                 * The first new event in binlog version 4 is Format_desc. So
                 * any event type after that does not exist in older versions.
                 * We use the events known by version 3, even if version 1 had
                 * only a subset of them (this is not a problem: it uses a few
                 * bytes for nothing but unifies code; it does not make the
                 * slave detect less corruptions).
                 */
                numberOfEventTypes = FORMAT_DESCRIPTION_EVENT - 1;

                postHeaderLen[START_EVENT_V3 - 1] = START_V3_HEADER_LEN;
                postHeaderLen[QUERY_EVENT - 1] = QUERY_HEADER_MINIMAL_LEN;
                postHeaderLen[ROTATE_EVENT - 1] = ROTATE_HEADER_LEN;
                postHeaderLen[LOAD_EVENT - 1] = LOAD_HEADER_LEN;
                postHeaderLen[CREATE_FILE_EVENT - 1] = CREATE_FILE_HEADER_LEN;
                postHeaderLen[APPEND_BLOCK_EVENT - 1] = APPEND_BLOCK_HEADER_LEN;
                postHeaderLen[EXEC_LOAD_EVENT - 1] = EXEC_LOAD_HEADER_LEN;
                postHeaderLen[DELETE_FILE_EVENT - 1] = DELETE_FILE_HEADER_LEN;
                postHeaderLen[NEW_LOAD_EVENT - 1] = postHeaderLen[LOAD_EVENT - 1];
                break;

            case 1: /* 3.23 */
                /*
                 * We build an artificial (i.e. not sent by the master) event,
                 * which describes what those old master versions send.
                 */
                serverVersion = "3.23";
                commonHeaderLen = OLD_HEADER_LEN;

                /*
                 * The first new event in binlog version 4 is Format_desc. So
                 * any event type after that does not exist in older versions.
                 * We use the events known by version 3, even if version 1 had
                 * only a subset of them (this is not a problem: it uses a few
                 * bytes for nothing but unifies code; it does not make the
                 * slave detect less corruptions).
                 */
                numberOfEventTypes = FORMAT_DESCRIPTION_EVENT - 1;

                postHeaderLen[START_EVENT_V3 - 1] = START_V3_HEADER_LEN;
                postHeaderLen[QUERY_EVENT - 1] = QUERY_HEADER_MINIMAL_LEN;
                postHeaderLen[LOAD_EVENT - 1] = LOAD_HEADER_LEN;
                postHeaderLen[CREATE_FILE_EVENT - 1] = CREATE_FILE_HEADER_LEN;
                postHeaderLen[APPEND_BLOCK_EVENT - 1] = APPEND_BLOCK_HEADER_LEN;
                postHeaderLen[EXEC_LOAD_EVENT - 1] = EXEC_LOAD_HEADER_LEN;
                postHeaderLen[DELETE_FILE_EVENT - 1] = DELETE_FILE_HEADER_LEN;
                postHeaderLen[NEW_LOAD_EVENT - 1] = postHeaderLen[LOAD_EVENT - 1];
                break;

            default:
                numberOfEventTypes = 0;
                commonHeaderLen = 0;
        }
    }

    public void calcServerVersionSplit() {
        doServerVersionSplit(serverVersion, serverVersionSplit);
    }

    public long getVersionProduct() {
        return versionProduct(serverVersionSplit);
    }

    public boolean isVersionBeforeChecksum() {
        return getVersionProduct() < checksumVersionProduct;
    }

    public static void doServerVersionSplit(String serverVersion, int[] versionSplit) {
        String[] split = serverVersion.split("\\.");
        if (split.length < 3) {
            versionSplit[0] = 0;
            versionSplit[1] = 0;
            versionSplit[2] = 0;
        } else {
            int j = 0;
            for (int i = 0; i <= 2; i++) {
                String str = split[i];
                for (j = 0; j < str.length(); j++) {
                    if (Character.isDigit(str.charAt(j)) == false) {
                        break;
                    }
                }
                if (j > 0) {
                    versionSplit[i] = Integer.valueOf(str.substring(0, j), 10);
                } else {
                    versionSplit[0] = 0;
                    versionSplit[1] = 0;
                    versionSplit[2] = 0;
                }
            }
        }
    }

    public static long versionProduct(int[] versionSplit) {
        return ((versionSplit[0] * 256 + versionSplit[1]) * 256 + versionSplit[2]);
    }

    public final int getCommonHeaderLen() {
        return commonHeaderLen;
    }

    public final short[] getPostHeaderLen() {
        return postHeaderLen;
    }

}

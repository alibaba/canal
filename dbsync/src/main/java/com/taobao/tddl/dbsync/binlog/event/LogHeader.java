package com.taobao.tddl.dbsync.binlog.event;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * The Common-Header, documented in the table @ref Table_common_header "below",
 * always has the same form and length within one version of MySQL. Each event
 * type specifies a format and length of the Post-Header. The length of the
 * Common-Header is the same for all events of the same type. The Body may be of
 * different format and length even for different events of the same type. The
 * binary formats of Post-Header and Body are documented separately in each
 * subclass. The binary format of Common-Header is as follows.
 * <table>
 * <caption>Common-Header</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>4 byte unsigned integer</td>
 * <td>The time when the query started, in seconds since 1970.</td>
 * </tr>
 * <tr>
 * <td>type</td>
 * <td>1 byte enumeration</td>
 * <td>See enum #Log_event_type.</td>
 * </tr>
 * <tr>
 * <td>server_id</td>
 * <td>4 byte unsigned integer</td>
 * <td>Server ID of the server that created the event.</td>
 * </tr>
 * <tr>
 * <td>total_size</td>
 * <td>4 byte unsigned integer</td>
 * <td>The total size of this event, in bytes. In other words, this is the sum
 * of the sizes of Common-Header, Post-Header, and Body.</td>
 * </tr>
 * <tr>
 * <td>master_position</td>
 * <td>4 byte unsigned integer</td>
 * <td>The position of the next event in the master binary log, in bytes from
 * the beginning of the file. In a binlog that is not a relay log, this is just
 * the position of the next event, in bytes from the beginning of the file. In a
 * relay log, this is the position of the next event in the master's binlog.</td>
 * </tr>
 * <tr>
 * <td>flags</td>
 * <td>2 byte bitfield</td>
 * <td>See Log_event::flags.</td>
 * </tr>
 * </table>
 * Summing up the numbers above, we see that the total size of the common header
 * is 19 bytes.
 * 
 * @see mysql-5.1.60/sql/log_event.cc
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class LogHeader {

    protected final int type;

    /**
     * The offset in the log where this event originally appeared (it is
     * preserved in relay logs, making SHOW SLAVE STATUS able to print
     * coordinates of the event in the master's binlog). Note: when a
     * transaction is written by the master to its binlog (wrapped in
     * BEGIN/COMMIT) the log_pos of all the queries it contains is the one of
     * the BEGIN (this way, when one does SHOW SLAVE STATUS it sees the offset
     * of the BEGIN, which is logical as rollback may occur), except the COMMIT
     * query which has its real offset.
     */
    protected long      logPos;

    /**
     * Timestamp on the master(for debugging and replication of
     * NOW()/TIMESTAMP). It is important for queries and LOAD DATA INFILE. This
     * is set at the event's creation time, except for Query and Load (et al.)
     * events where this is set at the query's execution time, which guarantees
     * good replication (otherwise, we could have a query and its event with
     * different timestamps).
     */
    protected long      when;

    /** Number of bytes written by write() function */
    protected int       eventLen;

    /**
     * The master's server id (is preserved in the relay log; used to prevent
     * from infinite loops in circular replication).
     */
    protected long      serverId;

    /**
     * Some 16 flags. See the definitions above for LOG_EVENT_TIME_F,
     * LOG_EVENT_FORCED_ROTATE_F, LOG_EVENT_THREAD_SPECIFIC_F, and
     * LOG_EVENT_SUPPRESS_USE_F for notes.
     */
    protected int       flags;

    /**
     * The value is set by caller of FD constructor and
     * Log_event::write_header() for the rest. In the FD case it's propagated
     * into the last byte of post_header_len[] at FD::write(). On the slave side
     * the value is assigned from post_header_len[last] of the last seen FD
     * event.
     */
    protected int       checksumAlg;
    /**
     * Placeholder for event checksum while writing to binlog.
     */
    protected long      crc;        // ha_checksum

    /**
     * binlog fileName
     */
    protected String    logFileName;

    protected Map<String, String> gtidMap = new HashMap<>();

    private static final String CURRENT_GTID_STRING = "curt_gtid";
    private static final String CURRENT_GTID_SN = "curt_gtid_sn";
    private static final String CURRENT_GTID_LAST_COMMIT = "curt_gtid_lct";
    private static final String GTID_SET_STRING = "gtid_str";

    /* for Start_event_v3 */
    public LogHeader(final int type){
        this.type = type;
    }

    public LogHeader(LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        when = buffer.getUint32();
        type = buffer.getUint8(); // LogEvent.EVENT_TYPE_OFFSET;
        serverId = buffer.getUint32(); // LogEvent.SERVER_ID_OFFSET;
        eventLen = (int) buffer.getUint32(); // LogEvent.EVENT_LEN_OFFSET;

        if (descriptionEvent.binlogVersion == 1) {
            logPos = 0;
            flags = 0;
            return;
        }

        /* 4.0 or newer */
        logPos = buffer.getUint32(); // LogEvent.LOG_POS_OFFSET
        /*
         * If the log is 4.0 (so here it can only be a 4.0 relay log read by the
         * SQL thread or a 4.0 master binlog read by the I/O thread), log_pos is
         * the beginning of the event: we transform it into the end of the
         * event, which is more useful. But how do you know that the log is 4.0:
         * you know it if description_event is version 3 *and* you are not
         * reading a Format_desc (remember that mysqlbinlog starts by assuming
         * that 5.0 logs are in 4.0 format, until it finds a Format_desc).
         */
        if (descriptionEvent.binlogVersion == 3 && type < LogEvent.FORMAT_DESCRIPTION_EVENT && logPos != 0) {
            /*
             * If log_pos=0, don't change it. log_pos==0 is a marker to mean
             * "don't change rli->group_master_log_pos" (see
             * inc_group_relay_log_pos()). As it is unreal log_pos, adding the
             * event len's is nonsense. For example, a fake Rotate event should
             * not have its log_pos (which is 0) changed or it will modify
             * Exec_master_log_pos in SHOW SLAVE STATUS, displaying a nonsense
             * value of (a non-zero offset which does not exist in the master's
             * binlog, so which will cause problems if the user uses this value
             * in CHANGE MASTER).
             */
            logPos += eventLen; /* purecov: inspected */
        }

        flags = buffer.getUint16(); // LogEvent.FLAGS_OFFSET
        if ((type == LogEvent.FORMAT_DESCRIPTION_EVENT) || (type == LogEvent.ROTATE_EVENT)) {
            /*
             * These events always have a header which stops here (i.e. their
             * header is FROZEN).
             */
            /*
             * Initialization to zero of all other Log_event members as they're
             * not specified. Currently there are no such members; in the future
             * there will be an event UID (but Format_description and Rotate
             * don't need this UID, as they are not propagated through
             * --log-slave-updates (remember the UID is used to not play a query
             * twice when you have two masters which are slaves of a 3rd
             * master). Then we are done.
             */

            if (type == LogEvent.FORMAT_DESCRIPTION_EVENT) {
                int commonHeaderLen = buffer.getUint8(FormatDescriptionLogEvent.LOG_EVENT_MINIMAL_HEADER_LEN
                                                      + FormatDescriptionLogEvent.ST_COMMON_HEADER_LEN_OFFSET);
                buffer.position(commonHeaderLen + FormatDescriptionLogEvent.ST_SERVER_VER_OFFSET);
                String serverVersion = buffer.getFixString(FormatDescriptionLogEvent.ST_SERVER_VER_LEN); // ST_SERVER_VER_OFFSET
                int versionSplit[] = new int[] { 0, 0, 0 };
                FormatDescriptionLogEvent.doServerVersionSplit(serverVersion, versionSplit);
                checksumAlg = LogEvent.BINLOG_CHECKSUM_ALG_UNDEF;
                if (FormatDescriptionLogEvent.versionProduct(versionSplit) >= FormatDescriptionLogEvent.checksumVersionProduct) {
                    buffer.position(eventLen - LogEvent.BINLOG_CHECKSUM_LEN - LogEvent.BINLOG_CHECKSUM_ALG_DESC_LEN);
                    checksumAlg = buffer.getUint8();
                }

                processCheckSum(buffer);
            }
            return;
        }

        /*
         * CRC verification by SQL and Show-Binlog-Events master side. The
         * caller has to provide @description_event->checksum_alg to be the last
         * seen FD's (A) descriptor. If event is FD the descriptor is in it.
         * Notice, FD of the binlog can be only in one instance and therefore
         * Show-Binlog-Events executing master side thread needs just to know
         * the only FD's (A) value - whereas RL can contain more. In the RL
         * case, the alg is kept in FD_e (@description_event) which is reset to
         * the newer read-out event after its execution with possibly new alg
         * descriptor. Therefore in a typical sequence of RL: {FD_s^0, FD_m,
         * E_m^1} E_m^1 will be verified with (A) of FD_m. See legends
         * definition on MYSQL_BIN_LOG::relay_log_checksum_alg docs lines
         * (log.h). Notice, a pre-checksum FD version forces alg :=
         * BINLOG_CHECKSUM_ALG_UNDEF.
         */
        checksumAlg = descriptionEvent.getHeader().checksumAlg; // fetch
                                                                // checksum alg
        processCheckSum(buffer);
        /* otherwise, go on with reading the header from buf (nothing now) */
    }

    /**
     * The different types of log events.
     */
    public final int getType() {
        return type;
    }

    /**
     * The position of the next event in the master binary log, in bytes from
     * the beginning of the file. In a binlog that is not a relay log, this is
     * just the position of the next event, in bytes from the beginning of the
     * file. In a relay log, this is the position of the next event in the
     * master's binlog.
     */
    public final long getLogPos() {
        return logPos;
    }

    /**
     * The total size of this event, in bytes. In other words, this is the sum
     * of the sizes of Common-Header, Post-Header, and Body.
     */
    public final int getEventLen() {
        return eventLen;
    }

    /**
     * The time when the query started, in seconds since 1970.
     */
    public final long getWhen() {
        return when;
    }

    /**
     * Server ID of the server that created the event.
     */
    public final long getServerId() {
        return serverId;
    }

    /**
     * Some 16 flags. See the definitions above for LOG_EVENT_TIME_F,
     * LOG_EVENT_FORCED_ROTATE_F, LOG_EVENT_THREAD_SPECIFIC_F, and
     * LOG_EVENT_SUPPRESS_USE_F for notes.
     */
    public final int getFlags() {
        return flags;
    }

    public long getCrc() {
        return crc;
    }

    public int getChecksumAlg() {
        return checksumAlg;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    private void processCheckSum(LogBuffer buffer) {
        if (checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_OFF && checksumAlg != LogEvent.BINLOG_CHECKSUM_ALG_UNDEF) {
            crc = buffer.getUint32(eventLen - LogEvent.BINLOG_CHECKSUM_LEN);
        }
    }

    public String getGtidSetStr() {
        return gtidMap.get(GTID_SET_STRING);
    }

    public String getCurrentGtid() {
        return gtidMap.get(CURRENT_GTID_STRING);
    }

    public String getCurrentGtidSn() {
        return gtidMap.get(CURRENT_GTID_SN);
    }

    public String getCurrentGtidLastCommit() {
        return gtidMap.get(CURRENT_GTID_LAST_COMMIT);
    }

    public void putGtid(GTIDSet gtidSet, LogEvent gtidEvent) {
        if (gtidSet != null) {
            gtidMap.put(GTID_SET_STRING, gtidSet.toString());
            if (gtidEvent != null && gtidEvent instanceof GtidLogEvent) {
                GtidLogEvent event = (GtidLogEvent)gtidEvent;
                gtidMap.put(CURRENT_GTID_STRING, event.getGtidStr());
                gtidMap.put(CURRENT_GTID_SN, String.valueOf(event.getSequenceNumber()));
                gtidMap.put(CURRENT_GTID_LAST_COMMIT, String.valueOf(event.getLastCommitted()));
            }
        }
    }
}

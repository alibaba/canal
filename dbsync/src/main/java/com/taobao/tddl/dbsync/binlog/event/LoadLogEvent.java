package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * This log event corresponds to a "LOAD DATA INFILE" SQL query on the following
 * form:
 * 
 * <pre>
 *    (1)    USE db;
 *    (2)    LOAD DATA [CONCURRENT] [LOCAL] INFILE 'file_name'
 *    (3)    [REPLACE | IGNORE]
 *    (4)    INTO TABLE 'table_name'
 *    (5)    [FIELDS
 *    (6)      [TERMINATED BY 'field_term']
 *    (7)      [[OPTIONALLY] ENCLOSED BY 'enclosed']
 *    (8)      [ESCAPED BY 'escaped']
 *    (9)    ]
 *   (10)    [LINES
 *   (11)      [TERMINATED BY 'line_term']
 *   (12)      [LINES STARTING BY 'line_start']
 *   (13)    ]
 *   (14)    [IGNORE skip_lines LINES]
 *   (15)    (field_1, field_2, ..., field_n)
 * </pre>
 * 
 * Binary Format: The Post-Header consists of the following six components.
 * <table>
 * <caption>Post-Header for Load_log_event</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>slave_proxy_id</td>
 * <td>4 byte unsigned integer</td>
 * <td>An integer identifying the client thread that issued the query. The id is
 * unique per server. (Note, however, that two threads on different servers may
 * have the same slave_proxy_id.) This is used when a client thread creates a
 * temporary table local to the client. The slave_proxy_id is used to
 * distinguish temporary tables that belong to different clients.</td>
 * </tr>
 * <tr>
 * <td>exec_time</td>
 * <td>4 byte unsigned integer</td>
 * <td>The time from when the query started to when it was logged in the binlog,
 * in seconds.</td>
 * </tr>
 * <tr>
 * <td>skip_lines</td>
 * <td>4 byte unsigned integer</td>
 * <td>The number on line (14) above, if present, or 0 if line (14) is left out.
 * </td>
 * </tr>
 * <tr>
 * <td>table_name_len</td>
 * <td>1 byte unsigned integer</td>
 * <td>The length of 'table_name' on line (4) above.</td>
 * </tr>
 * <tr>
 * <td>db_len</td>
 * <td>1 byte unsigned integer</td>
 * <td>The length of 'db' on line (1) above.</td>
 * </tr>
 * <tr>
 * <td>num_fields</td>
 * <td>4 byte unsigned integer</td>
 * <td>The number n of fields on line (15) above.</td>
 * </tr>
 * </table>
 * The Body contains the following components.
 * <table>
 * <caption>Body of Load_log_event</caption>
 * <tr>
 * <th>Name</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>sql_ex</td>
 * <td>variable length</td>
 * <td>Describes the part of the query on lines (3) and (5)&ndash;(13) above.
 * More precisely, it stores the five strings (on lines) field_term (6),
 * enclosed (7), escaped (8), line_term (11), and line_start (12); as well as a
 * bitfield indicating the presence of the keywords REPLACE (3), IGNORE (3), and
 * OPTIONALLY (7). The data is stored in one of two formats, called "old" and
 * "new". The type field of Common-Header determines which of these two formats
 * is used: type LOAD_EVENT means that the old format is used, and type
 * NEW_LOAD_EVENT means that the new format is used. When MySQL writes a
 * Load_log_event, it uses the new format if at least one of the five strings is
 * two or more bytes long. Otherwise (i.e., if all strings are 0 or 1 bytes
 * long), the old format is used. The new and old format differ in the way the
 * five strings are stored.
 * <ul>
 * <li>In the new format, the strings are stored in the order field_term,
 * enclosed, escaped, line_term, line_start. Each string consists of a length (1
 * byte), followed by a sequence of characters (0-255 bytes). Finally, a boolean
 * combination of the following flags is stored in 1 byte: REPLACE_FLAG==0x4,
 * IGNORE_FLAG==0x8, and OPT_ENCLOSED_FLAG==0x2. If a flag is set, it indicates
 * the presence of the corresponding keyword in the SQL query.
 * <li>In the old format, we know that each string has length 0 or 1. Therefore,
 * only the first byte of each string is stored. The order of the strings is the
 * same as in the new format. These five bytes are followed by the same 1 byte
 * bitfield as in the new format. Finally, a 1 byte bitfield called empty_flags
 * is stored. The low 5 bits of empty_flags indicate which of the five strings
 * have length 0. For each of the following flags that is set, the corresponding
 * string has length 0; for the flags that are not set, the string has length 1:
 * FIELD_TERM_EMPTY==0x1, ENCLOSED_EMPTY==0x2, LINE_TERM_EMPTY==0x4,
 * LINE_START_EMPTY==0x8, ESCAPED_EMPTY==0x10.
 * </ul>
 * Thus, the size of the new format is 6 bytes + the sum of the sizes of the
 * five strings. The size of the old format is always 7 bytes.</td>
 * </tr>
 * <tr>
 * <td>field_lens</td>
 * <td>num_fields 1 byte unsigned integers</td>
 * <td>An array of num_fields integers representing the length of each field in
 * the query. (num_fields is from the Post-Header).</td>
 * </tr>
 * <tr>
 * <td>fields</td>
 * <td>num_fields null-terminated strings</td>
 * <td>An array of num_fields null-terminated strings, each representing a field
 * in the query. (The trailing zero is redundant, since the length are stored in
 * the num_fields array.) The total length of all strings equals to the sum of
 * all field_lens, plus num_fields bytes for all the trailing zeros.</td>
 * </tr>
 * <tr>
 * <td>table_name</td>
 * <td>null-terminated string of length table_len+1 bytes</td>
 * <td>The 'table_name' from the query, as a null-terminated string. (The
 * trailing zero is actually redundant since the table_len is known from
 * Post-Header.)</td>
 * </tr>
 * <tr>
 * <td>db</td>
 * <td>null-terminated string of length db_len+1 bytes</td>
 * <td>The 'db' from the query, as a null-terminated string. (The trailing zero
 * is actually redundant since the db_len is known from Post-Header.)</td>
 * </tr>
 * <tr>
 * <td>file_name</td>
 * <td>variable length string without trailing zero, extending to the end of the
 * event (determined by the length field of the Common-Header)</td>
 * <td>The 'file_name' from the query.</td>
 * </tr>
 * </table>
 * This event type is understood by current versions, but only generated by
 * MySQL 3.23 and earlier.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public class LoadLogEvent extends LogEvent {

    private String          table;
    private String          db;
    private String          fname;
    private int             skipLines;
    private int             numFields;
    private String[]        fields;

    /* sql_ex_info */
    private String          fieldTerm;
    private String          lineTerm;
    private String          lineStart;
    private String          enclosed;
    private String          escaped;
    private int             optFlags;
    private int             emptyFlags;

    private long            execTime;

    /* Load event post-header */
    public static final int L_THREAD_ID_OFFSET  = 0;
    public static final int L_EXEC_TIME_OFFSET  = 4;
    public static final int L_SKIP_LINES_OFFSET = 8;
    public static final int L_TBL_LEN_OFFSET    = 12;
    public static final int L_DB_LEN_OFFSET     = 13;
    public static final int L_NUM_FIELDS_OFFSET = 14;
    public static final int L_SQL_EX_OFFSET     = 18;
    public static final int L_DATA_OFFSET       = FormatDescriptionLogEvent.LOAD_HEADER_LEN;

    /*
     * These are flags and structs to handle all the LOAD DATA INFILE options
     * (LINES TERMINATED etc). DUMPFILE_FLAG is probably useless (DUMPFILE is a
     * clause of SELECT, not of LOAD DATA).
     */
    public static final int DUMPFILE_FLAG       = 0x1;
    public static final int OPT_ENCLOSED_FLAG   = 0x2;
    public static final int REPLACE_FLAG        = 0x4;
    public static final int IGNORE_FLAG         = 0x8;

    public static final int FIELD_TERM_EMPTY    = 0x1;
    public static final int ENCLOSED_EMPTY      = 0x2;
    public static final int LINE_TERM_EMPTY     = 0x4;
    public static final int LINE_START_EMPTY    = 0x8;
    public static final int ESCAPED_EMPTY       = 0x10;

    public LoadLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int loadHeaderLen = FormatDescriptionLogEvent.LOAD_HEADER_LEN;
        /*
         * I (Guilhem) manually tested replication of LOAD DATA INFILE for
         * 3.23->5.0, 4.0->5.0 and 5.0->5.0 and it works.
         */
        copyLogEvent(buffer,
            ((header.type == LOAD_EVENT) ? loadHeaderLen + descriptionEvent.commonHeaderLen : loadHeaderLen
                                                                                              + FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN),
            descriptionEvent);
    }

    /**
     * @see mysql-5.1.60/sql/log_event.cc - Load_log_event::copy_log_event
     */
    protected final void copyLogEvent(LogBuffer buffer, final int bodyOffset, FormatDescriptionLogEvent descriptionEvent) {
        /* this is the beginning of the post-header */
        buffer.position(descriptionEvent.commonHeaderLen + L_EXEC_TIME_OFFSET);

        execTime = buffer.getUint32(); // L_EXEC_TIME_OFFSET
        skipLines = (int) buffer.getUint32(); // L_SKIP_LINES_OFFSET
        final int tableNameLen = buffer.getUint8(); // L_TBL_LEN_OFFSET
        final int dbLen = buffer.getUint8(); // L_DB_LEN_OFFSET
        numFields = (int) buffer.getUint32(); // L_NUM_FIELDS_OFFSET

        buffer.position(bodyOffset);
        /*
         * Sql_ex.init() on success returns the pointer to the first byte after
         * the sql_ex structure, which is the start of field lengths array.
         */
        if (header.type != LOAD_EVENT /* use_new_format */) {
            /*
             * The code below assumes that buf will not disappear from under our
             * feet during the lifetime of the event. This assumption holds true
             * in the slave thread if the log is in new format, but is not the
             * case when we have old format because we will be reusing net
             * buffer to read the actual file before we write out the
             * Create_file event.
             */
            fieldTerm = buffer.getString();
            enclosed = buffer.getString();
            lineTerm = buffer.getString();
            lineStart = buffer.getString();
            escaped = buffer.getString();
            optFlags = buffer.getInt8();
            emptyFlags = 0;
        } else {
            fieldTerm = buffer.getFixString(1);
            enclosed = buffer.getFixString(1);
            lineTerm = buffer.getFixString(1);
            lineStart = buffer.getFixString(1);
            escaped = buffer.getFixString(1);
            optFlags = buffer.getUint8();
            emptyFlags = buffer.getUint8();

            if ((emptyFlags & FIELD_TERM_EMPTY) != 0) fieldTerm = null;
            if ((emptyFlags & ENCLOSED_EMPTY) != 0) enclosed = null;
            if ((emptyFlags & LINE_TERM_EMPTY) != 0) lineTerm = null;
            if ((emptyFlags & LINE_START_EMPTY) != 0) lineStart = null;
            if ((emptyFlags & ESCAPED_EMPTY) != 0) escaped = null;
        }

        final int fieldLenPos = buffer.position();
        buffer.forward(numFields);
        fields = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            final int fieldLen = buffer.getUint8(fieldLenPos + i);
            fields[i] = buffer.getFixString(fieldLen + 1);
        }

        table = buffer.getFixString(tableNameLen + 1);
        db = buffer.getFixString(dbLen + 1);

        // null termination is accomplished by the caller
        final int from = buffer.position();
        final int end = from + buffer.limit();
        int found = from;
        for (; (found < end) && buffer.getInt8(found) != '\0'; found++)
            /* empty loop */;
        fname = buffer.getString(found);
        buffer.forward(1); // The + 1 is for \0 terminating fname
    }

    public final String getTable() {
        return table;
    }

    public final String getDb() {
        return db;
    }

    public final String getFname() {
        return fname;
    }

    public final int getSkipLines() {
        return skipLines;
    }

    public final String[] getFields() {
        return fields;
    }

    public final String getFieldTerm() {
        return fieldTerm;
    }

    public final String getLineTerm() {
        return lineTerm;
    }

    public final String getLineStart() {
        return lineStart;
    }

    public final String getEnclosed() {
        return enclosed;
    }

    public final String getEscaped() {
        return escaped;
    }

    public final int getOptFlags() {
        return optFlags;
    }

    public final int getEmptyFlags() {
        return emptyFlags;
    }

    public final long getExecTime() {
        return execTime;
    }
}

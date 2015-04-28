package com.taobao.tddl.dbsync.binlog.event;

import java.io.IOException;

import com.taobao.tddl.dbsync.binlog.LogBuffer;

/**
 * Event responsible for LOAD DATA execution, it similar to Query_log_event but
 * before executing the query it substitutes original filename in LOAD DATA
 * query with name of temporary file.
 * <ul>
 * <li>4 bytes. The ID of the file to load.</li>
 * <li>4 bytes. The start position within the statement for filename
 * substitution.</li>
 * <li>4 bytes. The end position within the statement for filename substitution.
 * </li>
 * <li>1 byte. How to handle duplicates: LOAD_DUP_ERROR = 0, LOAD_DUP_IGNORE =
 * 1, LOAD_DUP_REPLACE = 2</li>
 * </ul>
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class ExecuteLoadQueryLogEvent extends QueryLogEvent {

    /** file_id of temporary file */
    private long            fileId;

    /** pointer to the part of the query that should be substituted */
    private int             fnPosStart;

    /** pointer to the end of this part of query */
    private int             fnPosEnd;

    /*
     * Elements of this enum describe how LOAD DATA handles duplicates.
     */
    public static final int LOAD_DUP_ERROR          = 0;
    public static final int LOAD_DUP_IGNORE         = LOAD_DUP_ERROR + 1;
    public static final int LOAD_DUP_REPLACE        = LOAD_DUP_IGNORE + 1;

    /**
     * We have to store type of duplicate handling explicitly, because for LOAD
     * DATA it also depends on LOCAL option. And this part of query will be
     * rewritten during replication so this information may be lost...
     */
    private int             dupHandling;

    /* ELQ = "Execute Load Query" */
    public static final int ELQ_FILE_ID_OFFSET      = QUERY_HEADER_LEN;
    public static final int ELQ_FN_POS_START_OFFSET = ELQ_FILE_ID_OFFSET + 4;
    public static final int ELQ_FN_POS_END_OFFSET   = ELQ_FILE_ID_OFFSET + 8;
    public static final int ELQ_DUP_HANDLING_OFFSET = ELQ_FILE_ID_OFFSET + 12;

    public ExecuteLoadQueryLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent)
                                                                                                                   throws IOException{
        super(header, buffer, descriptionEvent);

        buffer.position(descriptionEvent.commonHeaderLen + ELQ_FILE_ID_OFFSET);

        fileId = buffer.getUint32(); // ELQ_FILE_ID_OFFSET
        fnPosStart = (int) buffer.getUint32(); // ELQ_FN_POS_START_OFFSET
        fnPosEnd = (int) buffer.getUint32(); // ELQ_FN_POS_END_OFFSET
        dupHandling = buffer.getInt8(); // ELQ_DUP_HANDLING_OFFSET

        final int len = query.length();
        if (fnPosStart > len || fnPosEnd > len || dupHandling > LOAD_DUP_REPLACE) {
            throw new IOException(String.format("Invalid ExecuteLoadQueryLogEvent: fn_pos_start=%d, "
                                                + "fn_pos_end=%d, dup_handling=%d", fnPosStart, fnPosEnd, dupHandling));
        }
    }

    public final int getFilenamePosStart() {
        return fnPosStart;
    }

    public final int getFilenamePosEnd() {
        return fnPosEnd;
    }

    public final String getFilename() {
        if (query != null) return query.substring(fnPosStart, fnPosEnd).trim();

        return null;
    }

    public final long getFileId() {
        return fileId;
    }
}

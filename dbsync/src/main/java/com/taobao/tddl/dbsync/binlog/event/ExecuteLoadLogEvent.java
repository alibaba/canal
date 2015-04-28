package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * Execute_load_log_event.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class ExecuteLoadLogEvent extends LogEvent {

    private final long      fileId;

    /* EL = "Execute Load" */
    public static final int EL_FILE_ID_OFFSET = 0;

    public ExecuteLoadLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        buffer.position(commonHeaderLen + EL_FILE_ID_OFFSET);
        fileId = buffer.getUint32(); // EL_FILE_ID_OFFSET
    }

    public final long getFileId() {
        return fileId;
    }
}

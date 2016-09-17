package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;

/**
 * Log row deletions. The event contain several delete rows for a table. Note
 * that each event contains only rows for one table.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class DeleteRowsLogEvent extends RowsLogEvent {

    public DeleteRowsLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
    }
}

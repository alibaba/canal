package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * @author agapple 2022年5月23日 下午7:05:39
 * @version 1.1.6
 * @since mysql 8.0.20
 */
public class TransactionPayloadLogEvent extends LogEvent {

    public TransactionPayloadLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
    }
}

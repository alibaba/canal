package com.taobao.tddl.dbsync.binlog.event.mariadb;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;

/**
 * mariadb的Start_encryption_log_event
 * 
 * @author agapple 2018年5月7日 下午7:23:02
 * @version 1.0.26
 */
public class StartEncryptionLogEvent extends LogEvent {

    public StartEncryptionLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
    }
}

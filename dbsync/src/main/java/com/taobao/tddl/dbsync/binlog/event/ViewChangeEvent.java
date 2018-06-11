package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * @author agapple 2018年5月7日 下午7:05:39
 * @version 1.0.26
 * @since mysql 5.7
 */
public class ViewChangeEvent extends LogEvent {

    public ViewChangeEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
    }
}

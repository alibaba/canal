package com.taobao.tddl.dbsync.binlog.event.mariadb;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.IgnorableLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;

/**
 * mariadb的GTID_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午4:49:10
 * @since 1.0.17
 */
public class MariaGtidLogEvent extends IgnorableLogEvent {

    private long gtid;

    /**
     * <pre>
     * mariadb gtidlog event format
     *     uint<8> GTID sequence
     *     uint<4> Replication Domain ID
     *     uint<1> Flags
     * 
     * 	if flag & FL_GROUP_COMMIT_ID
     * 	    uint<8> commit_id
     * 	else
     * 	    uint<6> 0
     * </pre>
     */

    public MariaGtidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
        gtid = buffer.getUlong64().longValue();
        // do nothing , just ignore log event
    }

    public long getGtid() {
        return gtid;
    }

}

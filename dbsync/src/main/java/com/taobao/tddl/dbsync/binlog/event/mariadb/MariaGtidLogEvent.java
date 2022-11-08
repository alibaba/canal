package com.taobao.tddl.dbsync.binlog.event.mariadb;

import com.alibaba.otter.canal.parse.driver.mysql.packets.MariaGtid;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.IgnorableLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;

/**
 * mariadb的GTID_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午4:49:10
 * @since 1.0.17
 */
public class MariaGtidLogEvent extends LogEvent {

    private MariaGtid mariaGtid;

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
        super(header);
        long sequence = buffer.getUlong64().longValue();
        long domainId = buffer.getUint32();
        long serverId = header.getServerId();
        mariaGtid = new MariaGtid(domainId, serverId, sequence);
    }

    public String getGtidStr() {
        return mariaGtid.toString();
    }
}

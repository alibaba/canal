package com.taobao.tddl.dbsync.binlog.event.mariadb;

import com.alibaba.otter.canal.parse.driver.mysql.packets.MariaGTIDSet;
import com.alibaba.otter.canal.parse.driver.mysql.packets.MariaGtid;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;
import com.taobao.tddl.dbsync.binlog.event.IgnorableLogEvent;
import com.taobao.tddl.dbsync.binlog.event.LogHeader;

/**
 * mariadb的GTID_LIST_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午4:51:50
 * @since 1.0.17
 */
public class MariaGtidListLogEvent extends LogEvent {

    private MariaGTIDSet mariaGTIDSet;
    /**
     * <pre>
     * mariadb gtidListLog event format
     *  uint<4> Number of GTIDs
     *  GTID[0]
     *      uint<4> Replication Domain ID
     *      uint<4> Server_ID
     *      uint<8> GTID sequence ...
     * GTID[n]
     * </pre>
     * @param header
     * @param buffer
     * @param descriptionEvent
     */
    public MariaGtidListLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);
        long gtidLenth = buffer.getUint32();
        mariaGTIDSet = new MariaGTIDSet();
        for (int i = 0; i < gtidLenth; i++) {
            long domainId = buffer.getUint32();
            long serverId = buffer.getUint32();
            long sequence = buffer.getUlong64().longValue();
            mariaGTIDSet.add(new MariaGtid(domainId, serverId, sequence));
        }
    }

    public String getGtidStr() {
        return mariaGTIDSet.toString();
    }
}

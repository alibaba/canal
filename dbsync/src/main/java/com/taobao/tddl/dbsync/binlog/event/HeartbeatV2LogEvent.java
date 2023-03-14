package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * <pre>
 *   Replication event to ensure to replica that source is alive.
 *   The event is originated by source's dump thread and sent straight to
 *   replica without being logged. Slave itself does not store it in relay log
 *   but rather uses a data for immediate checks and throws away the event.
 *   Two members of the class m_log_filename and m_log_position comprise
 *   @see the rpl_event_coordinates instance. The coordinates that a heartbeat
 *   instance carries correspond to the last event source has sent from
 *   its binlog.
 *   Also this event will be generated only for the source server with
 *   version > 8.0.26
 * </pre>
 * 
 * @author jianghang 2022-09-01 下午16:36:29
 * @version 1.1.6
 * @since mysql 8.0.26
 */
public class HeartbeatV2LogEvent extends LogEvent {

    private byte[]          payload;

    public HeartbeatV2LogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        int payloadLenth = buffer.limit() - commonHeaderLen;
        // see : https://github.com/mysql/mysql-server/commit/59e590738a772b74ad50c4a57c86aaa1bc6501c7#diff-184e9a7d8a58f080974e475d4199fe5c6da5518c8a2811cc5df5988c8f9e9797
        payload = buffer.getData(payloadLenth);
    }

    public byte[] getPayload() {
        return payload;
    }
}

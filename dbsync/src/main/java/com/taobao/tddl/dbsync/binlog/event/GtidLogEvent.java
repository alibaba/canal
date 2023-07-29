package com.taobao.tddl.dbsync.binlog.event;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * @author jianghang 2013-4-8 上午12:36:29
 * @version 1.0.3
 * @since mysql 5.6 / mariadb10
 */
public class GtidLogEvent extends LogEvent {

    // / Length of the commit_flag in event encoding
    public static final int ENCODED_FLAG_LENGTH         = 1;
    // / Length of SID in event encoding
    public static final int ENCODED_SID_LENGTH          = 16;
    public static final int LOGICAL_TIMESTAMP_TYPE_CODE = 2;
    public static final UUID UUID_ZERO                   = new UUID(0, 0);

    private boolean         commitFlag;
    private UUID            sid;
    private long            gno;
    private long            lastCommitted;
    private long            sequenceNumber;

    public GtidLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        // final int postHeaderLen = descriptionEvent.postHeaderLen[header.type
        // - 1];

        buffer.position(commonHeaderLen);
        commitFlag = (buffer.getUint8() != 0); // ENCODED_FLAG_LENGTH

        long high = buffer.getBeLong64();
        long low = buffer.getBeLong64();
        if (high == 0 && low == 0) {
            sid = UUID_ZERO;
        } else {
            sid = new UUID(high, low);
        }

        gno = buffer.getLong64();

        // support gtid lastCommitted and sequenceNumber
        // fix bug #776
        if (buffer.hasRemaining() && buffer.remaining() > 16 && buffer.getUint8() == LOGICAL_TIMESTAMP_TYPE_CODE) {
            lastCommitted = buffer.getLong64();
            sequenceNumber = buffer.getLong64();
        }

        // ignore gtid info read
        // sid.copy_from((uchar *)ptr_buffer);
        // ptr_buffer+= ENCODED_SID_LENGTH;
        //
        // // SIDNO is only generated if needed, in get_sidno().
        // spec.gtid.sidno= -1;
        //
        // spec.gtid.gno= uint8korr(ptr_buffer);
        // ptr_buffer+= ENCODED_GNO_LENGTH;
    }

    public boolean isCommitFlag() {
        return commitFlag;
    }

    public UUID getSid() {
        return sid;
    }

    public long getGno() {
        return gno;
    }

    public long getLastCommitted() {
        return lastCommitted;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getGtidStr() {
        StringBuilder sb = new StringBuilder();
        sb.append(sid.toString()).append(":");
        sb.append(gno);
        return sb.toString();
    }
}

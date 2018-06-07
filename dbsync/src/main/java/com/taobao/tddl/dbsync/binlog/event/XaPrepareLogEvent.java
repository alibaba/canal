package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * @author agapple 2018年5月7日 下午7:05:39
 * @version 1.0.26
 * @since mysql 5.7
 */
public class XaPrepareLogEvent extends LogEvent {

    private boolean onePhase;
    private int     formatId;
    private int     gtridLength;
    private int     bqualLength;
    private byte[]  data;

    public XaPrepareLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.getCommonHeaderLen();
        final int postHeaderLen = descriptionEvent.getPostHeaderLen()[header.getType() - 1];

        int offset = commonHeaderLen + postHeaderLen;
        buffer.position(offset);

        onePhase = (buffer.getInt8() == 0x00 ? false : true);

        formatId = buffer.getInt32();
        gtridLength = buffer.getInt32();
        bqualLength = buffer.getInt32();

        int MY_XIDDATASIZE = 128;
        if (MY_XIDDATASIZE >= gtridLength + bqualLength && gtridLength >= 0 && gtridLength <= 64 && bqualLength >= 0
            && bqualLength <= 64) {
            data = buffer.getData(gtridLength + bqualLength);
        } else {
            formatId = -1;
            gtridLength = 0;
            bqualLength = 0;
        }
    }

    public boolean isOnePhase() {
        return onePhase;
    }

    public int getFormatId() {
        return formatId;
    }

    public int getGtridLength() {
        return gtridLength;
    }

    public int getBqualLength() {
        return bqualLength;
    }

    public byte[] getData() {
        return data;
    }

}

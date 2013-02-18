package com.alibaba.otter.canal.parse.driver.mysql.utils;

/**
 * MySQL Constants.<br>
 * constants that is used in mysql server.<br>
 * 
 * @author fujohnwang
 */
public abstract class MSC {

    public static final int  MAX_PACKET_LENGTH                  = (1 << 24);
    public static final int  HEADER_PACKET_LENGTH_FIELD_LENGTH  = 3;
    public static final int  HEADER_PACKET_LENGTH_FIELD_OFFSET  = 0;
    public static final int  HEADER_PACKET_LENGTH               = 4;
    public static final int  HEADER_PACKET_NUMBER_FIELD_LENGTH  = 1;

    public static final byte NULL_TERMINATED_STRING_DELIMITER   = 0x00;
    public static final byte DEFAULT_PROTOCOL_VERSION           = 0x0a;

    public static final int  FIELD_COUNT_FIELD_LENGTH           = 1;

    public static final int  EVENT_TYPE_OFFSET                  = 4;
    public static final int  EVENT_LEN_OFFSET                   = 9;

    public static final long DEFAULT_BINLOG_FILE_START_POSITION = 4;
}

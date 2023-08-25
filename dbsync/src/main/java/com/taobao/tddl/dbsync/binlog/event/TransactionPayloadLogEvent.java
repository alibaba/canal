package com.taobao.tddl.dbsync.binlog.event;

import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogEvent;

/**
 * @author agapple 2022年5月23日 下午7:05:39
 * @version 1.1.7
 * @since mysql 8.0.20
 */
public class TransactionPayloadLogEvent extends LogEvent {

    public static final short COMPRESSION_TYPE_MIN_LENGTH         = 1;
    public static final short COMPRESSION_TYPE_MAX_LENGTH         = 9;
    public static final short PAYLOAD_SIZE_MIN_LENGTH             = 0;
    public static final short PAYLOAD_SIZE_MAX_LENGTH             = 9;
    public static final short UNCOMPRESSED_SIZE_MIN_LENGTH        = 0;
    public static final short UNCOMPRESSED_SIZE_MAX_LENGTH        = 9;
    public static final int   MAX_DATA_LENGTH                     = COMPRESSION_TYPE_MAX_LENGTH
                                                                    + PAYLOAD_SIZE_MAX_LENGTH
                                                                    + UNCOMPRESSED_SIZE_MAX_LENGTH;

    /** Marks the end of the payload header. */
    public static final int   OTW_PAYLOAD_HEADER_END_MARK         = 0;

    /** The payload field */
    public static final int   OTW_PAYLOAD_SIZE_FIELD              = 1;

    /** The compression type field */
    public static final int   OTW_PAYLOAD_COMPRESSION_TYPE_FIELD  = 2;

    /** The uncompressed size field */
    public static final int   OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3;

    /* ZSTD compression. */
    public final static int   COMPRESS_TYPE_ZSTD                  = 0;
    /* No compression. */
    public final static int   COMPRESS_TYPE_NONE                  = 255;

    private long              m_compression_type                  = COMPRESS_TYPE_NONE;
    private long              m_payload_size;
    private long              m_uncompressed_size;
    private byte[]            m_payload;

    public TransactionPayloadLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header);

        final int commonHeaderLen = descriptionEvent.getCommonHeaderLen();
        final int postHeaderLen = descriptionEvent.getPostHeaderLen()[header.getType() - 1];

        int offset = commonHeaderLen;
        buffer.position(offset);
        long type = 0, length = 0;
        while (buffer.hasRemaining()) {
            type = buffer.getPackedLong(); // type
            if (type == OTW_PAYLOAD_HEADER_END_MARK) {
                break;
            }

            length = buffer.getPackedLong(); // length
            switch ((int) type) {
                case OTW_PAYLOAD_SIZE_FIELD:
                    m_payload_size = buffer.getPackedLong(); // value
                    break;
                case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
                    m_compression_type = buffer.getPackedLong(); // value
                    break;
                case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
                    m_uncompressed_size = buffer.getPackedLong(); // value
                    break;
                default:
                    buffer.forward((int) length);
                    break;
            }

        }

        if (m_uncompressed_size == 0) {
            m_uncompressed_size = m_payload_size;
        }
        m_payload = buffer.getData((int) m_payload_size);
    }

    public boolean isCompressByZstd() {
        return m_compression_type == COMPRESS_TYPE_ZSTD;
    }

    public boolean isCompressByNone() {
        return m_compression_type == COMPRESS_TYPE_NONE;
    }

    public byte[] getPayload() {
        return m_payload;
    }
}

package com.taobao.tddl.dbsync.binlog;


/**
 * Position inside binlog file
 *
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class BinlogPosition extends LogPosition {

    /* The source server_id of position, 0 invalid */
    protected final long masterId;

    /* The timestamp, in seconds, 0 invalid */
    protected final long timestamp;

    public BinlogPosition(String fileName, long position, long masterId, long timestamp){
        super(fileName, position);
        this.masterId = masterId;
        this.timestamp = timestamp;
    }

    public BinlogPosition(LogPosition logPosition, long masterId, long timestamp){
        super(logPosition.getFileName(), logPosition.getPosition());
        this.masterId = masterId;
        this.timestamp = timestamp;
    }

    public BinlogPosition(BinlogPosition binlogPosition){
        super(binlogPosition.getFileName(), binlogPosition.getPosition());
        this.masterId = binlogPosition.masterId;
        this.timestamp = binlogPosition.timestamp;
    }

    private final static long[] pow10 = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
            10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L, 1000000000000000L,
            10000000000000000L, 100000000000000000L, 1000000000000000000L };

    public static String placeHolder(int bit, long number) {
        if (bit > 18) {
            throw new IllegalArgumentException("Bit must less than 18, but given " + bit);
        }

        final long max = pow10[bit];
        if (number >= max) {
            // 当 width < 数值的最大位数时，应该直接返回数值
            return String.valueOf(number);
        }

        return String.valueOf(max + number).substring(1);
    }

    public String format2String(final int positionMaxLen) {
        String binlogSuffix = fileName;
        String binlogOffset = placeHolder((int) positionMaxLen, position);
        // 输出 '000001:0000000004@12+12314130'
        StringBuffer buf = new StringBuffer(40);
        buf.append(binlogSuffix);
        buf.append(':');
        buf.append(binlogOffset);
        if (masterId != 0) {
            buf.append('#');
            buf.append(masterId);
        }
        if (timestamp != 0) {
            buf.append('.');
            buf.append(timestamp);
        }
        return buf.toString();
    }

    public static BinlogPosition parseFromString(String source) {
        int colonIndex = source.indexOf(':');
        int miscIndex = colonIndex + 1;
        int sharpIndex = source.indexOf('#', miscIndex);
        int semicolonIndex = source.indexOf(';', miscIndex); // NOTE: 向后兼容
        int dotIndex = source.lastIndexOf('.');
        if (colonIndex == -1) {
            return null; // NOTE: 错误的位点
        }

        String binlogSuffix = source.substring(0, colonIndex);
        long binlogPosition;
        if (sharpIndex != -1) {
            binlogPosition = Long.parseLong(source.substring(miscIndex, sharpIndex));
        } else if (semicolonIndex != -1) {
            binlogPosition = Long.parseLong(source.substring(miscIndex, semicolonIndex)); // NOTE:
                                                                                          // 向后兼容
        } else if (dotIndex != -1) {
            binlogPosition = Long.parseLong(source.substring(miscIndex, dotIndex));
        } else {
            binlogPosition = Long.parseLong(source.substring(miscIndex));
        }

        long masterId = 0; // NOTE: 默认值为 0
        if (sharpIndex != -1) {
            if (dotIndex != -1) {
                masterId = Long.parseLong(source.substring(sharpIndex + 1, dotIndex));
            } else {
                masterId = Long.parseLong(source.substring(sharpIndex + 1));
            }
        }

        long timestamp = 0; // NOTE: 默认值为 0
        if (dotIndex != -1 && dotIndex > colonIndex) {
            timestamp = Long.parseLong(source.substring(dotIndex + 1));
        }

        return new BinlogPosition(binlogSuffix, binlogPosition, // NL
            masterId,
            timestamp);
    }

    public String getFilePattern() {
        final int index = fileName.indexOf('.');
        if (index != -1) {
            return fileName.substring(0, index);
        }
        return null;
    }

    public void setFilePattern(String filePattern) {
        // We tolerate the event ID with or without the binlog prefix.
        if (fileName.indexOf('.') < 0) {
            fileName = filePattern + '.' + fileName;
        }
    }

    public long getMasterId() {
        return masterId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return format2String(10);
    }
}

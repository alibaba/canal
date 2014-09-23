package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

public class ErrorPacket extends PacketWithHeaderPacket {

    public byte   fieldCount;
    public int    errorNumber;
    public byte   sqlStateMarker;
    public byte[] sqlState;
    public String message;

    /**
     * <pre>
     * VERSION 4.1
     *  Bytes                       Name
     *  -----                       ----
     *  1                           field_count, always = 0xff
     *  2                           errno
     *  1                           (sqlstate marker), always '#'
     *  5                           sqlstate (5 characters)
     *  n                           message
     * 
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read field count
        this.fieldCount = data[0];
        index++;
        // 2. read error no.
        this.errorNumber = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 3. read marker
        this.sqlStateMarker = data[index];
        index++;
        // 4. read sqlState
        this.sqlState = ByteHelper.readFixedLengthBytes(data, index, 5);
        index += 5;
        // 5. read message
        this.message = new String(ByteHelper.readFixedLengthBytes(data, index, data.length - index));
        // end read
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

    @Override
    public String toString() {
        return "ErrorPacket [errorNumber=" + errorNumber + ", fieldCount=" + fieldCount + ", message=" + message
               + ", sqlState=" + sqlStateToString() + ", sqlStateMarker=" + (char) sqlStateMarker + "]";
    }

    private String sqlStateToString() {
        StringBuilder builder = new StringBuilder(5);
        for (byte b : this.sqlState) {
            builder.append((char) b);
        }
        return builder.toString();
    }

}

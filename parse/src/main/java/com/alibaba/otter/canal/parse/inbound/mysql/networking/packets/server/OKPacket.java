package com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.server;

import java.io.IOException;

import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.support.ByteHelper;

/**
 * Aka. OK packet
 * 
 * @author fujohnwang
 */
public class OKPacket extends PacketWithHeaderPacket {

    public byte   fieldCount;
    public byte[] affectedRows;
    public byte[] insertId;
    public int    serverStatus;
    public int    warningCount;
    public String message;

    /**
     * <pre>
     *  VERSION 4.1
     *  Bytes                       Name
     *  -----                       ----
     *  1   (Length Coded Binary)   field_count, always = 0
     *  1-9 (Length Coded Binary)   affected_rows
     *  1-9 (Length Coded Binary)   insert_id
     *  2                           server_status
     *  2                           warning_count
     *  n   (until end of packet)   message
     * </pre>
     * 
     * @throws IOException
     */
    public void fromBytes(byte[] data) throws IOException {
        int index = 0;
        // 1. read field count
        this.fieldCount = data[0];
        index++;
        // 2. read affected rows
        this.affectedRows = ByteHelper.readBinaryCodedLengthBytes(data, index);
        index += this.affectedRows.length;
        // 3. read insert id
        this.insertId = ByteHelper.readBinaryCodedLengthBytes(data, index);
        index += this.insertId.length;
        // 4. read server status
        this.serverStatus = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 5. read warning count
        this.warningCount = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 6. read message.
        this.message = new String(ByteHelper.readFixedLengthBytes(data, index, data.length - index));
        // end read
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

}

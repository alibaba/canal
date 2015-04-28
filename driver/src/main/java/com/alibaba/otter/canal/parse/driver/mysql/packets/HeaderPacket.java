package com.alibaba.otter.canal.parse.driver.mysql.packets;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * <pre>
 * Offset  Length     Description
 *   0       3        Packet body length stored with the low byte first.
 *   3       1        Packet sequence number. The sequence numbers are reset with each new command. 
 *                      While the correct packet sequencing is ensured by the underlying transmission protocol,
 *                      this field is used for the sanity checks of the application logic.
 * </pre>
 * 
 * <br>
 * The Packet Header will not be shown in the descriptions of packets that
 * follow this section. Think of it as always there. But logically, it
 * "precedes the packet" rather than "is included in the packet".<br>
 * 
 * @author fujohnwang
 */
public class HeaderPacket implements IPacket {

    /**
     * this field indicates the packet length that follows the header, with
     * header packet's 4 bytes excluded.
     */
    private int  packetBodyLength;
    private byte packetSequenceNumber;

    /**
     * little-endian byte order
     */
    public byte[] toBytes() {
        byte[] data = new byte[4];
        data[0] = (byte) (packetBodyLength & 0xFF);
        data[1] = (byte) (packetBodyLength >>> 8);
        data[2] = (byte) (packetBodyLength >>> 16);
        data[3] = getPacketSequenceNumber();
        return data;
    }

    /**
     * little-endian byte order
     */
    public void fromBytes(byte[] data) {
        if (data == null || data.length != 4) {
            throw new IllegalArgumentException("invalid header data. It can't be null and the length must be 4 byte.");
        }
        this.packetBodyLength = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8) | ((data[2] & 0xFF) << 16);
        this.setPacketSequenceNumber(data[3]);
    }

    public int getPacketBodyLength() {
        return packetBodyLength;
    }

    public void setPacketBodyLength(int packetBodyLength) {
        this.packetBodyLength = packetBodyLength;
    }

    public void setPacketSequenceNumber(byte packetSequenceNumber) {
        this.packetSequenceNumber = packetSequenceNumber;
    }

    public byte getPacketSequenceNumber() {
        return packetSequenceNumber;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}

package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * COM_REGISTER_SLAVE
 * 
 * @author zhibinliu
 * @since 1.0.24
 */
public class RegisterSlaveCommandPacket extends CommandPacket {

    public String reportHost;
    public int    reportPort;
    public String reportUser;
    public String reportPasswd;
    public long   serverId;

    public RegisterSlaveCommandPacket(){
        setCommand((byte) 0x15);
    }

    public void fromBytes(byte[] data) {
        // bypass
    }

    public static byte[] toLH(int n) {
        byte[] b = new byte[4];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        return b;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        ByteHelper.writeUnsignedIntLittleEndian(serverId, out);
        out.write((byte) reportHost.getBytes().length);
        ByteHelper.writeFixedLengthBytesFromStart(reportHost.getBytes(), reportHost.getBytes().length, out);
        out.write((byte) reportUser.getBytes().length);
        ByteHelper.writeFixedLengthBytesFromStart(reportUser.getBytes(), reportUser.getBytes().length, out);
        out.write((byte) reportPasswd.getBytes().length);
        ByteHelper.writeFixedLengthBytesFromStart(reportPasswd.getBytes(), reportPasswd.getBytes().length, out);
        ByteHelper.writeUnsignedShortLittleEndian(reportPort, out);
        ByteHelper.writeUnsignedIntLittleEndian(0, out);// Fake
                                                        // rpl_recovery_rank
        ByteHelper.writeUnsignedIntLittleEndian(0, out);// master id
        return out.toByteArray();
    }

}

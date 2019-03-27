package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

public class AuthSwitchRequestMoreData extends CommandPacket {

    public int    status;
    public byte[] authData;

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read status
        status = data[index];
        index += 1;
        authData = ByteHelper.readNullTerminatedBytes(data, index);
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

}

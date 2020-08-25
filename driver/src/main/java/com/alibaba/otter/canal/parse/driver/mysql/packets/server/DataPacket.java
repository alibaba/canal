package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;

public class DataPacket extends CommandPacket {

    @Override
    public void fromBytes(byte[] data) {

    }

    @Override
    public byte[] toBytes() throws IOException {
        return null;
    }

}

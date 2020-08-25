package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

public class Reply323Packet extends PacketWithHeaderPacket {

    public byte[] seed;

    @Override
    public void fromBytes(byte[] data) throws IOException {

    }

    @Override
    public byte[] toBytes() throws IOException {
        if (seed == null) {
            return new byte[] { (byte) 0 };
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteHelper.writeNullTerminated(seed, out);
            return out.toByteArray();
        }
    }

}

package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.packets.IPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * @author 枭博
 * @date 2024/05/14
 */
public class SslRequestCommandPacket implements IPacket {

    private final int serverCharsetNumber;

    public SslRequestCommandPacket(int serverCharsetNumber){
        this.serverCharsetNumber = serverCharsetNumber;
    }

    @Override
    public void fromBytes(byte[] data) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int clientCapabilities = Capability.CLIENT_LONG_FLAG | Capability.CLIENT_PROTOCOL_41
                                 | Capability.CLIENT_SECURE_CONNECTION | Capability.CLIENT_PLUGIN_AUTH
                                 | Capability.CLIENT_SSL;
        ByteHelper.writeUnsignedIntLittleEndian(clientCapabilities, out);
        ByteHelper.writeUnsignedIntLittleEndian(0, out);
        out.write(serverCharsetNumber);
        for (int i = 0; i < 23; i++) {
            out.write(0);
        }
        return out.toByteArray();
    }
}

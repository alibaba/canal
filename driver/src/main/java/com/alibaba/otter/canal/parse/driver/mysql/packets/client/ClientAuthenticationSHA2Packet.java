package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MSC;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MySQLPasswordEncrypter;

import org.apache.commons.lang.StringUtils;

public class ClientAuthenticationSHA2Packet extends ClientAuthenticationPacket {

    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 1. write client_flags
        int clientCapability = 0;
        clientCapability |= Capability.CLIENT_LONG_PASSWORD;
        clientCapability |= Capability.CLIENT_LONG_FLAG;
        clientCapability |= Capability.CLIENT_PROTOCOL_41;
        clientCapability |= Capability.CLIENT_TRANSACTIONS;
        clientCapability |= Capability.CLIENT_SECURE_CONNECTION;
        clientCapability |= Capability.CLIENT_MULTI_STATEMENTS;
        clientCapability |= Capability.CLIENT_PLUGIN_AUTH;
        clientCapability |= Capability.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        if (getDatabaseName() != null) {
            clientCapability |= Capability.CLIENT_CONNECT_WITH_DB;
        }
        ByteHelper.writeUnsignedIntLittleEndian(clientCapability, out);

        // 2. write max_packet_size
        ByteHelper.writeUnsignedIntLittleEndian(MSC.MAX_PACKET_LENGTH, out);
        // 3. write charset_number
        out.write(getCharsetNumber());
        // 4. write (filler) always 0x00...
        out.write(new byte[23]);
        // 5. write (Null-Terminated String) user
        ByteHelper.writeNullTerminatedString(getUsername(), out);
        // 6. write (Length Coded Binary) scramble_buff (1 + x bytes)
        if (StringUtils.isEmpty(getPassword())) {
            out.write(0x00);
        } else {
            try {
                byte[] encryptedPassword = MySQLPasswordEncrypter.scrambleCachingSha2(getPassword().getBytes(),
                    getScrumbleBuff());
                ByteHelper.writeBinaryCodedLengthBytes(encryptedPassword, out);
            } catch (Exception e) {
                throw new IOException("can't encrypt password that will be sent to MySQL server.", e);
            }
        }
        // 7 . (Null-Terminated String) databasename (optional)
        if (getDatabaseName() != null) {
            ByteHelper.writeNullTerminatedString(getDatabaseName(), out);
        }
        // 8 . (Null-Terminated String) auth plugin name (optional)
        if (getAuthPluginName() != null) {
            ByteHelper.writeNullTerminated(getAuthPluginName(), out);
        }
        // end write
        return out.toByteArray();
    }

}

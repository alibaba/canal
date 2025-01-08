package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MSC;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MySQLPasswordEncrypter;

public class ClientAuthenticationSHA2Packet extends ClientAuthenticationPacket {

    private int clientCapability = Capability.CLIENT_LONG_PASSWORD | Capability.CLIENT_LONG_FLAG
                                   | Capability.CLIENT_PROTOCOL_41 | Capability.CLIENT_INTERACTIVE
                                   | Capability.CLIENT_TRANSACTIONS | Capability.CLIENT_SECURE_CONNECTION
                                   | Capability.CLIENT_MULTI_STATEMENTS | Capability.CLIENT_PLUGIN_AUTH
                                   | Capability.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;

    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 1. write client_flags
        int capability = clientCapability;
        if (getDatabaseName() != null) {
            capability |= Capability.CLIENT_CONNECT_WITH_DB;
        }
        ByteHelper.writeUnsignedIntLittleEndian(capability, out);

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

package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.driver.mysql.packets.Capability;
import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MSC;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MySQLPasswordEncrypter;

public class ClientAuthenticationPacket extends PacketWithHeaderPacket {

    private int    clientCapability = Capability.CLIENT_LONG_PASSWORD | Capability.CLIENT_LONG_FLAG
                                      | Capability.CLIENT_PROTOCOL_41 | Capability.CLIENT_INTERACTIVE
                                      | Capability.CLIENT_TRANSACTIONS | Capability.CLIENT_SECURE_CONNECTION
                                      | Capability.CLIENT_MULTI_STATEMENTS | Capability.CLIENT_PLUGIN_AUTH;
    private String username;
    private String password;
    private byte   charsetNumber;
    private String databaseName;
    private int    serverCapabilities;
    private byte[] scrumbleBuff;
    private byte[] authPluginName;

    public void fromBytes(byte[] data) {
        // bypass since nowhere to use.
    }

    /**
     * <pre>
     * VERSION 4.1
     *  Bytes                        Name
     *  -----                        ----
     *  4                            client_flags
     *  4                            max_packet_size
     *  1                            charset_number
     *  23                           (filler) always 0x00...
     *  n (Null-Terminated String)   user
     *  n (Length Coded Binary)      scramble_buff (1 + x bytes)
     *  n (Null-Terminated String)   databasename (optional)
     *  n (Null-Terminated String)   auth plugin name (optional)
     * </pre>
     * 
     * @throws IOException
     */
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 1. write client_flags
        ByteHelper.writeUnsignedIntLittleEndian(clientCapability, out); // remove
        // client_interactive
        // feature

        // 2. write max_packet_size
        ByteHelper.writeUnsignedIntLittleEndian(MSC.MAX_PACKET_LENGTH, out);
        // 3. write charset_number
        out.write(this.charsetNumber);
        // 4. write (filler) always 0x00...
        out.write(new byte[23]);
        // 5. write (Null-Terminated String) user
        ByteHelper.writeNullTerminatedString(getUsername(), out);
        // 6. write (Length Coded Binary) scramble_buff (1 + x bytes)
        if (StringUtils.isEmpty(getPassword())) {
            out.write(0x00);
        } else {
            try {
                byte[] encryptedPassword = MySQLPasswordEncrypter.scramble411(getPassword().getBytes(), scrumbleBuff);
                ByteHelper.writeBinaryCodedLengthBytes(encryptedPassword, out);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setCharsetNumber(byte charsetNumber) {
        this.charsetNumber = charsetNumber;
    }

    public byte getCharsetNumber() {
        return charsetNumber;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        if (databaseName != null) {
            this.clientCapability |= Capability.CLIENT_CONNECT_WITH_DB;
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setServerCapabilities(int serverCapabilities) {
        this.serverCapabilities = serverCapabilities;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }

    public void setScrumbleBuff(byte[] scrumbleBuff) {
        this.scrumbleBuff = scrumbleBuff;
    }

    public byte[] getScrumbleBuff() {
        return scrumbleBuff;
    }

    public byte[] getAuthPluginName() {
        return authPluginName;
    }

    public void setAuthPluginName(byte[] authPluginName) {
        this.authPluginName = authPluginName;
        if (authPluginName != null) {
            this.clientCapability |= Capability.CLIENT_PLUGIN_AUTH;
        }
    }

}

package com.alibaba.otter.canal.parse.driver.mysql.packets.server;

import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.PacketWithHeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MSC;

/**
 * MySQL Handshake Initialization Packet.<br>
 * 
 * @author fujohnwang
 * @since 1.0
 */
public class HandshakeInitializationPacket extends PacketWithHeaderPacket {

    public byte   protocolVersion = MSC.DEFAULT_PROTOCOL_VERSION;
    public String serverVersion;
    public long   threadId;
    public byte[] seed;
    public int    serverCapabilities;
    public byte   serverCharsetNumber;
    public int    serverStatus;
    public byte[] restOfScrambleBuff;

    public HandshakeInitializationPacket(){
    }

    public HandshakeInitializationPacket(HeaderPacket header){
        super(header);
    }

    /**
     * <pre>
     * Bytes                        Name
     *  -----                        ----
     *  1                            protocol_version
     *  n (Null-Terminated String)   server_version
     *  4                            thread_id
     *  8                            scramble_buff
     *  1                            (filler) always 0x00
     *  2                            server_capabilities
     *  1                            server_language
     *  2                            server_status
     *  13                           (filler) always 0x00 ...
     *  13                           rest of scramble_buff (4.1)
     * </pre>
     */
    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read protocol_version
        protocolVersion = data[index];
        index++;
        // 2. read server_version
        byte[] serverVersionBytes = ByteHelper.readNullTerminatedBytes(data, index);
        serverVersion = new String(serverVersionBytes);
        index += (serverVersionBytes.length + 1);
        // 3. read thread_id
        threadId = ByteHelper.readUnsignedIntLittleEndian(data, index);
        index += 4;
        // 4. read scramble_buff
        seed = ByteHelper.readFixedLengthBytes(data, index, 8);
        index += 8;
        index += 1; // 1 byte (filler) always 0x00
        // 5. read server_capabilities
        this.serverCapabilities = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 6. read server_language
        this.serverCharsetNumber = data[index];
        index++;
        // 7. read server_status
        this.serverStatus = ByteHelper.readUnsignedShortLittleEndian(data, index);
        index += 2;
        // 8. bypass filtered bytes
        index += 13;
        // 9. read rest of scramble_buff
        this.restOfScrambleBuff = ByteHelper.readFixedLengthBytes(data, index, 12); // 虽然Handshake
                                                                                    // Initialization
        // Packet规定最后13个byte是剩下的scrumble,
        // 但实际上最后一个字节是0, 不应该包含在scrumble中.
        // end read
    }

    /**
     * Bypass implementing it, 'cause nowhere to use it.
     */
    public byte[] toBytes() throws IOException {
        return null;
    }

}

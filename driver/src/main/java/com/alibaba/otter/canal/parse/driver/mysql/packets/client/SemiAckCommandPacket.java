package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * semi ack command
 * 
 * @author amos_chen
 */
public class SemiAckCommandPacket extends CommandPacket {

    public long   binlogPosition;
    public String binlogFileName;

    public SemiAckCommandPacket(){

    }

    @Override
    public void fromBytes(byte[] data) throws IOException {
    }

    /**
     * <pre>
     * Bytes                        Name
     *  --------------------------------------------------------
     *  Bytes                        Name
     *  -----                        ----
     *  1                            semi mark
     *  8                            binlog position to start at (little endian)
     *  n                            binlog file name
     * 
     * </pre>
     */
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 0 write semi mark
        out.write(0xef);
        // 1 write 8 bytes for position
        ByteHelper.write8ByteUnsignedIntLittleEndian(binlogPosition, out);

        // 2 write binlog filename
        if (StringUtils.isNotEmpty(binlogFileName)) {
            out.write(binlogFileName.getBytes());
        }
        return out.toByteArray();
    }

}

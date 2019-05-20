package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.driver.mysql.utils.ByteHelper;

/**
 * Created by hiwjd on 2018/4/24. hiwjd0@gmail.com
 * https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
 */
public class BinlogDumpGTIDCommandPacket extends CommandPacket {

    public static final int BINLOG_DUMP_NON_BLOCK   = 0x01;
    public static final int BINLOG_THROUGH_POSITION = 0x02;
    public static final int BINLOG_THROUGH_GTID     = 0x04;

    public long             slaveServerId;
    public GTIDSet          gtidSet;

    public BinlogDumpGTIDCommandPacket(){
        setCommand((byte) 0x1e);
    }

    @Override
    public void fromBytes(byte[] data) throws IOException {
    }

    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        // 0. [1] write command number
        out.write(getCommand());
        // 1. [2] flags
        ByteHelper.writeUnsignedShortLittleEndian(BINLOG_THROUGH_GTID, out);
        // 2. [4] server-id
        ByteHelper.writeUnsignedIntLittleEndian(slaveServerId, out);
        // 3. [4] binlog-filename-len
        ByteHelper.writeUnsignedIntLittleEndian(0, out);
        // 4. [] binlog-filename
        // skip
        // 5. [8] binlog-pos
        ByteHelper.writeUnsignedInt64LittleEndian(4, out);
        // if flags & BINLOG_THROUGH_GTID {
        byte[] bs = gtidSet.encode();
        // 6. [4] data-size
        ByteHelper.writeUnsignedIntLittleEndian(bs.length, out);
        // 7, [] data
        // [8] n_sids // 文档写的是4个字节，其实是8个字节
        // for n_sids {
        // [16] SID
        // [8] n_intervals
        // for n_intervals {
        // [8] start (signed)
        // [8] end (signed)
        // }
        // }
        out.write(bs);
        // }

        return out.toByteArray();
    }
}

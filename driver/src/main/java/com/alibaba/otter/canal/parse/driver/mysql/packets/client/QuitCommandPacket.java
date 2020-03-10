package com.alibaba.otter.canal.parse.driver.mysql.packets.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.CommandPacket;

/**
 * quit cmd
 * 
 * @author agapple 2016年3月1日 下午8:33:02
 * @since 1.0.22
 */
public class QuitCommandPacket extends CommandPacket {

    public QuitCommandPacket(){
        setCommand((byte) 0x01);
    }

    @Override
    public void fromBytes(byte[] data) throws IOException {

    }

    @Override
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(getCommand());
        return out.toByteArray();
    }

}

package com.alibaba.otter.canal.parse.inbound.mysql.utils;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.client.QueryCommandPacket;
import com.alibaba.otter.canal.parse.support.PacketManager;

public class MysqlUpdateExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MysqlUpdateExecutor.class);

    private SocketChannel       channel;

    public MysqlUpdateExecutor(SocketChannel ch){
        this.channel = ch;
    }

    public void update(String updateString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(updateString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(channel, bodyBytes);

        logger.debug("read update result...");
        byte[] body = PacketManager.readBytes(channel, PacketManager.readHeader(channel, 4).getPacketBodyLength());
        if (body[0] < 0) {
            throw new IOException("something goes wrong when updating with command: " + updateString);
        }
    }
}

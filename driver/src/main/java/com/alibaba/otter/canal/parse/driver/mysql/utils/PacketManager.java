package com.alibaba.otter.canal.parse.driver.mysql.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;

public abstract class PacketManager {

    public static HeaderPacket readHeader(SocketChannel ch, int len) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.fromBytes(ch.read(len));
        return header;
    }

    public static HeaderPacket readHeader(SocketChannel ch, int len, int timeout) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.fromBytes(ch.read(len, timeout));
        return header;
    }

    public static byte[] readBytes(SocketChannel ch, int len) throws IOException {
        return ch.read(len);
    }

    public static byte[] readBytes(SocketChannel ch, int len, int timeout) throws IOException {
        return ch.read(len, timeout);
    }

    public static void writePkg(SocketChannel ch, byte[]... srcs) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] src : srcs) {
            out.write(src);
        }
        ch.write(out.toByteArray());
    }

    public static void writeBody(SocketChannel ch, byte[] body) throws IOException {
        writeBody0(ch, body, (byte) 0);
    }

    public static void writeBody0(SocketChannel ch, byte[] body, byte packetSeqNumber) throws IOException {
        HeaderPacket header = new HeaderPacket();
        header.setPacketBodyLength(body.length);
        header.setPacketSequenceNumber(packetSeqNumber);
        ch.write(header.toBytes(), body);
    }
}

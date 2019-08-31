package com.alibaba.otter.canal.admin.connector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.protocol.AdminPacket.Ack;
import com.alibaba.otter.canal.protocol.AdminPacket.ClientAuth;
import com.alibaba.otter.canal.protocol.AdminPacket.Handshake;
import com.alibaba.otter.canal.protocol.AdminPacket.InstanceAdmin;
import com.alibaba.otter.canal.protocol.AdminPacket.LogAdmin;
import com.alibaba.otter.canal.protocol.AdminPacket.Packet;
import com.alibaba.otter.canal.protocol.AdminPacket.PacketType;
import com.alibaba.otter.canal.protocol.AdminPacket.ServerAdmin;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;

/**
 * 基于netty实现的admin控制
 *
 * @author agapple 2019年8月26日 上午10:23:44
 * @since 1.1.4
 */
public class SimpleAdminConnector implements AdminConnector {

    private static final Logger logger      = LoggerFactory.getLogger(SimpleAdminConnector.class);
    private String              user;
    private String              passwd;
    private SocketAddress       address;
    private int                 soTimeout   = 60000;                                              // milliseconds
    private int                 idleTimeout = 60 * 60 * 1000;                                     // client和server之间的空闲链接超时的时间,默认为1小时
    private final ByteBuffer    readHeader  = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    private final ByteBuffer    writeHeader = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    private SocketChannel       channel;
    private ReadableByteChannel readableChannel;
    private WritableByteChannel writableChannel;
    private volatile boolean    connected   = false;

    public SimpleAdminConnector(String ip, int port, String user, String passwd){
        this.address = new InetSocketAddress(ip, port);
        this.user = user;
        this.passwd = passwd;
    }

    @Override
    public void connect() throws ServiceException {
        try {
            if (connected) {
                return;
            }

            channel = SocketChannel.open();
            channel.socket().setSoTimeout(soTimeout);
            channel.connect(address);
            readableChannel = Channels.newChannel(channel.socket().getInputStream());
            writableChannel = Channels.newChannel(channel.socket().getOutputStream());
            Packet p = Packet.parseFrom(readNextPacket());
            if (p.getVersion() != 1) {
                throw new CanalClientException("unsupported version at this client.");
            }

            if (p.getType() != PacketType.HANDSHAKE) {
                throw new CanalClientException("expect handshake but found other type.");
            }

            Handshake handshake = Handshake.parseFrom(p.getBody());
            ByteString seed = handshake.getSeeds(); // seed for auth
            String newPasswd = passwd;
            if (passwd != null) {
                // encode passwd
                newPasswd = SecurityUtil.byte2HexStr(SecurityUtil.scramble411(passwd.getBytes(), seed.toByteArray()));
            }

            ClientAuth ca = ClientAuth.newBuilder()
                .setUsername(user != null ? user : "")
                .setPassword(ByteString.copyFromUtf8(newPasswd != null ? newPasswd : ""))
                .setNetReadTimeout(idleTimeout)
                .setNetWriteTimeout(idleTimeout)
                .build();
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.CLIENTAUTHENTICATION)
                .setBody(ca.toByteString())
                .build()
                .toByteArray());
            //
            Packet ack = Packet.parseFrom(readNextPacket());
            if (ack.getType() != PacketType.ACK) {
                throw new CanalClientException("unexpected packet type when ack is expected");
            }

            Ack ackBody = Ack.parseFrom(ack.getBody());
            if (ackBody.getCode() > 0) {
                throw new ServiceException("something goes wrong when doing authentication: " + ackBody.getMessage());
            }

            connected = true;
        } catch (IOException e) {
            throw new ServiceException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public void disconnect() throws ServiceException {
        if (!connected) {
            return;
        }

        connected = false;
        if (readableChannel != null) {
            quietlyClose(readableChannel);
            readableChannel = null;
        }
        if (writableChannel != null) {
            quietlyClose(writableChannel);
            writableChannel = null;
        }
        if (channel != null) {
            quietlyClose(channel);
            channel = null;
        }
    }

    @Override
    public boolean check() {
        return BooleanUtils.toBoolean(Integer.parseInt(doServerAdmin("check")));
    }

    @Override
    public boolean start() {
        return BooleanUtils.toBoolean(Integer.parseInt(doServerAdmin("start")));
    }

    @Override
    public boolean stop() {
        return BooleanUtils.toBoolean(Integer.parseInt(doServerAdmin("stop")));
    }

    @Override
    public boolean restart() {
        return BooleanUtils.toBoolean(Integer.parseInt(doServerAdmin("restart")));
    }

    @Override
    public String getRunningInstances() {
        return doServerAdmin("list");
    }

    @Override
    public boolean checkInstance(String destination) {
        return BooleanUtils.toBoolean(Integer.parseInt(doInstanceAdmin(destination, "check")));
    }

    @Override
    public boolean startInstance(String destination) {
        return BooleanUtils.toBoolean(Integer.parseInt(doInstanceAdmin(destination, "start")));
    }

    @Override
    public boolean stopInstance(String destination) {
        return BooleanUtils.toBoolean(Integer.parseInt(doInstanceAdmin(destination, "stop")));
    }

    @Override
    public boolean releaseInstance(String destination) {
        return BooleanUtils.toBoolean(Integer.parseInt(doInstanceAdmin(destination, "release")));
    }

    @Override
    public boolean restartInstance(String destination) {
        return BooleanUtils.toBoolean(Integer.parseInt(doInstanceAdmin(destination, "restart")));
    }

    @Override
    public String listCanalLog() {
        return doLogAdmin("server", "list", null, null, 0);
    }

    @Override
    public String canalLog(int lines) {
        return doLogAdmin("server", "file", null, null, lines);
    }

    @Override
    public String listInstanceLog(String destination) {
        return doLogAdmin("instance", "list", destination, null, 0);
    }

    @Override
    public String instanceLog(String destination, String fileName, int lines) {
        return doLogAdmin("instance", "file", destination, fileName, lines);
    }

    // ==================== helper method ====================

    private String doServerAdmin(String action) {
        try {
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.SERVER)
                .setBody(ServerAdmin.newBuilder().setAction(action).build().toByteString())
                .build()
                .toByteArray());

            Packet p = Packet.parseFrom(readNextPacket());
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getCode() > 0) {
                throw new ServiceException("failed to subscribe with reason: " + ack.getMessage());
            }

            return ack.getMessage();
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    private String doInstanceAdmin(String destination, String action) {
        try {
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.INSTANCE)
                .setBody(InstanceAdmin.newBuilder()
                    .setDestination(destination)
                    .setAction(action)
                    .build()
                    .toByteString())
                .build()
                .toByteArray());

            Packet p = Packet.parseFrom(readNextPacket());
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getCode() > 0) {
                throw new ServiceException("failed to subscribe with reason: " + ack.getMessage());
            }

            return ack.getMessage();
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    private String doLogAdmin(String type, String action, String destination, String file, int count) {
        try {
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.LOG)
                .setBody(LogAdmin.newBuilder()
                    .setType(type)
                    .setAction(action)
                    .setDestination(destination == null ? "" : destination)
                    .setFile(file == null ? "" : file)
                    .setCount(count)
                    .build()
                    .toByteString())
                .build()
                .toByteArray());

            Packet p = Packet.parseFrom(readNextPacket());
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getCode() > 0) {
                throw new ServiceException("failed to subscribe with reason: " + ack.getMessage());
            }

            return ack.getMessage();
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    private void writeWithHeader(byte[] body) throws IOException {
        writeWithHeader(writableChannel, body);
    }

    private byte[] readNextPacket() throws IOException {
        return readNextPacket(readableChannel);
    }

    private void writeWithHeader(WritableByteChannel channel, byte[] body) throws IOException {
        writeHeader.clear();
        writeHeader.putInt(body.length);
        writeHeader.flip();
        channel.write(writeHeader);
        channel.write(ByteBuffer.wrap(body));
    }

    private byte[] readNextPacket(ReadableByteChannel channel) throws IOException {
        readHeader.clear();
        read(channel, readHeader);
        int bodyLen = readHeader.getInt(0);
        ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen).order(ByteOrder.BIG_ENDIAN);
        read(channel, bodyBuf);
        return bodyBuf.array();
    }

    private void read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int r = channel.read(buffer);
            if (r == -1) {
                throw new IOException("end of stream when reading header");
            }
        }
    }

    private void quietlyClose(Channel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("exception on closing channel:{} \n {}", channel, e);
        }
    }
}

package com.alibaba.otter.canal.parse.driver.mysql;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.AuthSwitchResponsePacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.ClientAuthenticationPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.QuitCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.AuthSwitchRequestMoreData;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.AuthSwitchRequestPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.HandshakeInitializationPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.Reply323Packet;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannelPool;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MSC;
import com.alibaba.otter.canal.parse.driver.mysql.utils.MySQLPasswordEncrypter;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;

/**
 * 基于mysql socket协议的链接实现
 * 
 * @author jianghang 2013-2-18 下午09:22:30
 * @version 1.0.1
 */
public class MysqlConnector {

    private static final Logger logger            = LoggerFactory.getLogger(MysqlConnector.class);
    private InetSocketAddress   address;
    private String              username;
    private String              password;

    private byte                charsetNumber     = 33;
    private String              defaultSchema;
    private int                 soTimeout         = 30 * 1000;
    private int                 connTimeout       = 5 * 1000;
    private int                 receiveBufferSize = 16 * 1024;
    private int                 sendBufferSize    = 16 * 1024;

    private SocketChannel       channel;
    private volatile boolean    dumping           = false;
    // mysql connectinnId
    private long                connectionId      = -1;
    private AtomicBoolean       connected         = new AtomicBoolean(false);
    // serverVersion
    private String              serverVersion;

    public static final int     timeout           = 5 * 1000;                                     // 5s

    public MysqlConnector(){
    }

    public MysqlConnector(InetSocketAddress address, String username, String password){
        String addr = address.getHostString();
        int port = address.getPort();
        this.address = new InetSocketAddress(addr, port);

        this.username = username;
        this.password = password;
    }

    public MysqlConnector(InetSocketAddress address, String username, String password, byte charsetNumber,
                          String defaultSchema){
        this(address, username, password);

        this.charsetNumber = charsetNumber;
        this.defaultSchema = defaultSchema;
    }

    public void connect() throws IOException {
        if (connected.compareAndSet(false, true)) {
            try {
                channel = SocketChannelPool.open(address);
                logger.info("connect MysqlConnection to {}...", address);
                negotiate(channel);
            } catch (Exception e) {
                disconnect();
                throw new IOException("connect " + this.address + " failure", e);
            }
        } else {
            logger.error("the channel can't be connected twice.");
        }
    }

    public void reconnect() throws IOException {
        disconnect();
        connect();
    }

    public void disconnect() throws IOException {
        if (connected.compareAndSet(true, false)) {
            try {
                if (channel != null) {
                    channel.close();
                }
                logger.info("disConnect MysqlConnection to {}...", address);
            } catch (Exception e) {
                throw new IOException("disconnect " + this.address + " failure", e);
            }

            // 执行一次quit
            if (dumping && connectionId >= 0) {
                MysqlConnector connector = null;
                try {
                    connector = this.fork();
                    connector.connect();
                    MysqlUpdateExecutor executor = new MysqlUpdateExecutor(connector);
                    executor.update("KILL CONNECTION " + connectionId);
                } catch (Exception e) {
                    // 忽略具体异常
                    logger.info("KILL DUMP " + connectionId + " failure", e);
                } finally {
                    if (connector != null) {
                        connector.disconnect();
                    }
                }

                dumping = false;
            }
        } else {
            logger.info("the channel {} is not connected", this.address);
        }
    }

    public boolean isConnected() {
        return this.channel != null && this.channel.isConnected();
    }

    public MysqlConnector fork() {
        MysqlConnector connector = new MysqlConnector();
        connector.setCharsetNumber(getCharsetNumber());
        connector.setDefaultSchema(getDefaultSchema());
        connector.setAddress(getAddress());
        connector.setPassword(password);
        connector.setUsername(getUsername());
        connector.setReceiveBufferSize(getReceiveBufferSize());
        connector.setSendBufferSize(getSendBufferSize());
        connector.setSoTimeout(getSoTimeout());
        connector.setConnTimeout(connTimeout);
        return connector;
    }

    public void quit() throws IOException {
        QuitCommandPacket quit = new QuitCommandPacket();
        byte[] cmdBody = quit.toBytes();

        HeaderPacket quitHeader = new HeaderPacket();
        quitHeader.setPacketBodyLength(cmdBody.length);
        quitHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(channel, quitHeader.toBytes(), cmdBody);
    }

    private void negotiate(SocketChannel channel) throws IOException {
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol
        HeaderPacket header = PacketManager.readHeader(channel, 4, timeout);
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
        if (body[0] < 0) {// check field_count
            if (body[0] == -1) {
                ErrorPacket error = new ErrorPacket();
                error.fromBytes(body);
                throw new IOException("handshake exception:\n" + error.toString());
            } else if (body[0] == -2) {
                throw new IOException("Unexpected EOF packet at handshake phase.");
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
            }
        }
        HandshakeInitializationPacket handshakePacket = new HandshakeInitializationPacket();
        handshakePacket.fromBytes(body);
        if (handshakePacket.protocolVersion != MSC.DEFAULT_PROTOCOL_VERSION) {
            // HandshakeV9
            auth323(channel, (byte) (header.getPacketSequenceNumber() + 1), handshakePacket.seed);
            return;
        }

        connectionId = handshakePacket.threadId; // 记录一下connection
        serverVersion = handshakePacket.serverVersion; // 记录serverVersion
        logger.info("handshake initialization packet received, prepare the client authentication packet to send");
        ClientAuthenticationPacket clientAuth = new ClientAuthenticationPacket();
        clientAuth.setCharsetNumber(charsetNumber);

        clientAuth.setUsername(username);
        clientAuth.setPassword(password);
        clientAuth.setServerCapabilities(handshakePacket.serverCapabilities);
        clientAuth.setDatabaseName(defaultSchema);
        clientAuth.setScrumbleBuff(joinAndCreateScrumbleBuff(handshakePacket));
        clientAuth.setAuthPluginName("mysql_native_password".getBytes());

        byte[] clientAuthPkgBody = clientAuth.toBytes();
        HeaderPacket h = new HeaderPacket();
        h.setPacketBodyLength(clientAuthPkgBody.length);
        h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));

        PacketManager.writePkg(channel, h.toBytes(), clientAuthPkgBody);
        logger.info("client authentication packet is sent out.");

        // check auth result
        header = null;
        header = PacketManager.readHeader(channel, 4);
        body = null;
        body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
        assert body != null;
        byte marker = body[0];
        if (marker == -2 || marker == 1) {
            byte[] authData = null;
            String pluginName = null;
            if (marker == 1) {
                AuthSwitchRequestMoreData packet = new AuthSwitchRequestMoreData();
                packet.fromBytes(body);
                authData = packet.authData;
            } else {
                AuthSwitchRequestPacket packet = new AuthSwitchRequestPacket();
                packet.fromBytes(body);
                authData = packet.authData;
                pluginName = packet.authName;
            }

            boolean isSha2Password = false;
            byte[] encryptedPassword = null;
            if (pluginName != null && "mysql_native_password".equals(pluginName)) {
                try {
                    encryptedPassword = MySQLPasswordEncrypter.scramble411(getPassword().getBytes(), authData);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                }
            } else if (pluginName != null && "caching_sha2_password".equals(pluginName)) {
                isSha2Password = true;
                try {
                    encryptedPassword = MySQLPasswordEncrypter.scrambleCachingSha2(getPassword().getBytes(), authData);
                } catch (DigestException e) {
                    throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                }
            }
            assert encryptedPassword != null;
            AuthSwitchResponsePacket responsePacket = new AuthSwitchResponsePacket();
            responsePacket.authData = encryptedPassword;
            byte[] auth = responsePacket.toBytes();

            h = new HeaderPacket();
            h.setPacketBodyLength(auth.length);
            h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));
            PacketManager.writePkg(channel, h.toBytes(), auth);
            logger.info("auth switch response packet is sent out.");

            header = null;
            header = PacketManager.readHeader(channel, 4);
            body = null;
            body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
            assert body != null;
            if (isSha2Password) {
                if (body[0] == 0x01 && body[1] == 0x04) {
                    // password auth failed
                    throw new IOException("caching_sha2_password Auth failed");
                }

                header = null;
                header = PacketManager.readHeader(channel, 4);
                body = null;
                body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
            }
        }

        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Client Authentication:" + err.toString());
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
            }
        }
    }

    private void auth323(SocketChannel channel, byte packetSequenceNumber, byte[] seed) throws IOException {
        // auth 323
        Reply323Packet r323 = new Reply323Packet();
        if (password != null && password.length() > 0) {
            r323.seed = MySQLPasswordEncrypter.scramble323(password, new String(seed)).getBytes();
        }
        byte[] b323Body = r323.toBytes();

        HeaderPacket h323 = new HeaderPacket();
        h323.setPacketBodyLength(b323Body.length);
        h323.setPacketSequenceNumber((byte) (packetSequenceNumber + 1));

        PacketManager.writePkg(channel, h323.toBytes(), b323Body);
        logger.info("client 323 authentication packet is sent out.");
        // check auth result
        HeaderPacket header = PacketManager.readHeader(channel, 4);
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength());
        assert body != null;
        switch (body[0]) {
            case 0:
                break;
            case -1:
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Client Authentication:" + err.toString());
            default:
                throw new IOException("unpexpected packet with field_count=" + body[0]);
        }
    }

    private byte[] joinAndCreateScrumbleBuff(HandshakeInitializationPacket handshakePacket) throws IOException {
        byte[] dest = new byte[handshakePacket.seed.length + handshakePacket.restOfScrambleBuff.length];
        System.arraycopy(handshakePacket.seed, 0, dest, 0, handshakePacket.seed.length);
        System.arraycopy(handshakePacket.restOfScrambleBuff,
            0,
            dest,
            handshakePacket.seed.length,
            handshakePacket.restOfScrambleBuff.length);
        return dest;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public byte getCharsetNumber() {
        return charsetNumber;
    }

    public void setCharsetNumber(byte charsetNumber) {
        this.charsetNumber = charsetNumber;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
    }

    public boolean isDumping() {
        return dumping;
    }

    public void setDumping(boolean dumping) {
        this.dumping = dumping;
    }

    public int getConnTimeout() {
        return connTimeout;
    }

    public void setConnTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
    }

    public String getPassword() {
        return password;
    }

    public String getServerVersion() {
        return serverVersion;
    }

}

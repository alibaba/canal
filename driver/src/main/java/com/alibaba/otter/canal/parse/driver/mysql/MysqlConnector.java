package com.alibaba.otter.canal.parse.driver.mysql;

import static com.alibaba.otter.canal.parse.driver.mysql.packets.Capability.CLIENT_SSL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.*;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.*;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannelPool;
import com.alibaba.otter.canal.parse.driver.mysql.ssl.SslInfo;
import com.alibaba.otter.canal.parse.driver.mysql.ssl.SslMode;
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

    public static final int     timeout           = 5 * 1000;                                     // 5s
    private static final Logger logger            = LoggerFactory.getLogger(MysqlConnector.class);
    private InetSocketAddress   address;
    private String              username;
    private String              password;
    private SslInfo             sslInfo;
    private String              defaultSchema;
    private int                 soTimeout         = 30 * 1000;
    private int                 connTimeout       = 5 * 1000;
    private int                 receiveBufferSize = 16 * 1024;
    private int                 sendBufferSize    = 16 * 1024;
    private SocketChannel       channel;
    private volatile boolean    dumping           = false;
    // mysql connectionId
    private long                connectionId      = -1;
    private AtomicBoolean       connected         = new AtomicBoolean(false);
    // serverVersion
    private String              serverVersion;

    public MysqlConnector(){
    }

    public MysqlConnector(InetSocketAddress address, String username, String password){
        String addr = address.getHostString();
        int port = address.getPort();
        this.address = new InetSocketAddress(addr, port);

        this.username = username;
        this.password = password;
    }

    public MysqlConnector(InetSocketAddress address, String username, String password, String defaultSchema){
        this(address, username, password);

        this.defaultSchema = defaultSchema;
    }

    public MysqlConnector(InetSocketAddress address, String username, String password, String defaultSchema,
                          SslInfo sslInfo){
        this(address, username, password, defaultSchema);
        this.sslInfo = sslInfo;
    }

    public void connect() throws IOException {
        if (connected.compareAndSet(false, true)) {
            try {
                channel = SocketChannelPool.open(address);
                logger.info("connect MysqlConnection to {}...", address);
                negotiate(channel);
                printSslStatus();
            } catch (Exception e) {
                disconnect();
                throw new IOException("connect " + this.address + " failure", e);
            }
        } else {
            logger.error("the channel can't be connected twice.");
        }
    }

    private void printSslStatus() {
        try {
            MysqlQueryExecutor executor = new MysqlQueryExecutor(this);
            ResultSetPacket result = executor.query("SHOW STATUS LIKE 'Ssl_version'");
            String sslVersion = "";
            if (result.getFieldValues() != null && result.getFieldValues().size() >= 2) {
                sslVersion = result.getFieldValues().get(1);
            }
            result = executor.query("SHOW STATUS LIKE 'Ssl_cipher'");
            String sslCipher = "";
            if (result.getFieldValues() != null && result.getFieldValues().size() >= 2) {
                sslCipher = result.getFieldValues().get(1);
            }
            logger.info("connect MysqlConnection in sslMode {}, Ssl_version:{}, Ssl_cipher:{}",
                (sslInfo != null ? sslInfo.getSslMode() : SslMode.DISABLED),
                sslVersion,
                sslCipher);
        } catch (Exception e) {
            logger.warn("Can't show SSL status, server may not standard MySQL server", e);
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
        connector.setDefaultSchema(getDefaultSchema());
        connector.setAddress(getAddress());
        connector.setPassword(password);
        connector.setUsername(getUsername());
        connector.setReceiveBufferSize(getReceiveBufferSize());
        connector.setSendBufferSize(getSendBufferSize());
        connector.setSoTimeout(getSoTimeout());
        connector.setConnTimeout(connTimeout);
        connector.setSslInfo(getSslInfo());
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
                throw new IOException("Unexpected packet with field_count=" + body[0]);
            }
        }
        HandshakeInitializationPacket handshakePacket = new HandshakeInitializationPacket();
        handshakePacket.fromBytes(body);
        // default utf8(33)
        byte serverCharsetNumber = (handshakePacket.serverCharsetNumber != 0) ? handshakePacket.serverCharsetNumber : 33;
        SslMode sslMode = sslInfo != null ? sslInfo.getSslMode() : SslMode.DISABLED;
        if (sslMode != SslMode.DISABLED) {
            boolean serverSupportSsl = (handshakePacket.serverCapabilities & CLIENT_SSL) > 0;
            if (!serverSupportSsl) {
                throw new IOException("MySQL Server does not support SSL: " + address + " serverCapabilities: "
                                      + handshakePacket.serverCapabilities);
            }
            byte[] sslPacket = new SslRequestCommandPacket(serverCharsetNumber).toBytes();
            HeaderPacket sslHeader = new HeaderPacket();
            sslHeader.setPacketBodyLength(sslPacket.length);
            sslHeader.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));
            header.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));
            PacketManager.writePkg(channel, sslHeader.toBytes(), sslPacket);
            channel = SocketChannelPool.connectSsl(channel, sslInfo);
            this.channel = channel;
        }
        if (handshakePacket.protocolVersion != MSC.DEFAULT_PROTOCOL_VERSION) {
            // HandshakeV9
            auth323(channel, (byte) (header.getPacketSequenceNumber() + 1), handshakePacket.seed);
            return;
        }

        connectionId = handshakePacket.threadId; // 记录一下connection
        serverVersion = handshakePacket.serverVersion; // 记录serverVersion
        logger.info("handshake initialization packet received, prepare the client authentication packet to send");
        // 某些老协议的 server 默认不返回 auth plugin，需要使用默认的 mysql_native_password
        String authPluginName = "mysql_native_password";
        if (handshakePacket.authPluginName != null && handshakePacket.authPluginName.length > 0) {
            authPluginName = new String(handshakePacket.authPluginName);
        }
        logger.info("auth plugin: {}", authPluginName);
        boolean isSha2Password = false;
        ClientAuthenticationPacket clientAuth;
        if ("caching_sha2_password".equals(authPluginName)) {
            clientAuth = new ClientAuthenticationSHA2Packet();
            isSha2Password = true;
        } else {
            clientAuth = new ClientAuthenticationPacket();
        }
        clientAuth.setCharsetNumber(serverCharsetNumber);

        clientAuth.setUsername(username);
        clientAuth.setPassword(password);
        clientAuth.setServerCapabilities(handshakePacket.serverCapabilities);
        clientAuth.setDatabaseName(defaultSchema);
        clientAuth.setScrumbleBuff(joinAndCreateScrumbleBuff(handshakePacket));
        clientAuth.setAuthPluginName(authPluginName.getBytes());

        byte[] clientAuthPkgBody = clientAuth.toBytes();
        HeaderPacket h = new HeaderPacket();
        h.setPacketBodyLength(clientAuthPkgBody.length);
        h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));

        PacketManager.writePkg(channel, h.toBytes(), clientAuthPkgBody);
        logger.info("client authentication packet is sent out.");

        // check auth result
        header = PacketManager.readHeader(channel, 4);
        body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
        assert body != null;
        byte marker = body[0];
        if (marker == -2 || marker == 1) {
            if (isSha2Password && body[1] == 3) {
                // sha2 auth ok
                logger.info("caching_sha2_password auth success.");
                header = PacketManager.readHeader(channel, 4);
                body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
            } else {
                byte[] authData = null;
                String pluginName = authPluginName;
                if (marker == 1) {
                    AuthSwitchRequestMoreData packet = new AuthSwitchRequestMoreData();
                    packet.fromBytes(body);
                    authData = packet.authData;
                } else {
                    AuthSwitchRequestPacket packet = new AuthSwitchRequestPacket();
                    packet.fromBytes(body);
                    authData = packet.authData;
                    pluginName = packet.authName;
                    logger.info("auth switch pluginName is {}.", pluginName);
                }

                byte[] encryptedPassword = null;
                if ("mysql_clear_password".equals(pluginName)) {
                    encryptedPassword = getPassword().getBytes();
                    header = authSwitchAfterAuth(encryptedPassword, header);
                    body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
                } else if (pluginName == null || "mysql_native_password".equals(pluginName)) {
                    try {
                        encryptedPassword = MySQLPasswordEncrypter.scramble411(getPassword().getBytes(), authData);
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                    }
                    header = authSwitchAfterAuth(encryptedPassword, header);
                    body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
                } else if ("caching_sha2_password".equals(pluginName)) {
                    if (body[0] == 0x01 && body[1] == 0x04) {
                        // support full auth
                        // clientAuth提前采用了sha2编码,会减少一次auth交互
                        header = cachingSha2PasswordFullAuth(channel,
                            header,
                            getPassword().getBytes(),
                            clientAuth.getScrumbleBuff());
                        body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
                    } else {
                        byte[] scramble = authData;
                        try {
                            encryptedPassword = MySQLPasswordEncrypter.scrambleCachingSha2(getPassword().getBytes(),
                                    scramble);
                        } catch (DigestException e) {
                            throw new RuntimeException("can't encrypt password that will be sent to MySQL server.", e);
                        }
                        header = authSwitchAfterAuth(encryptedPassword, header);
                        body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
                        assert body != null;
                        if (body[0] == 0x01 && body[1] == 0x04) {
                            // fixed issue https://github.com/alibaba/canal/pull/4767, support mysql 8.0.30+
                            header = cachingSha2PasswordFullAuth(channel, header, getPassword().getBytes(), scramble);
                            body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
                        } else {
                            header = PacketManager.readHeader(channel, 4);
                            body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
                        }
                    }
                } else {
                    header = authSwitchAfterAuth(encryptedPassword, header);
                    body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
                }
            }
        }
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket err = new ErrorPacket();
                err.fromBytes(body);
                throw new IOException("Error When doing Client Authentication:" + err.toString());
            } else {
                throw new IOException("Unexpected packet with field_count=" + body[0]);
            }
        }
    }

    private HeaderPacket cachingSha2PasswordFullAuth(SocketChannel channel, HeaderPacket header, byte[] pass,
                                                     byte[] seed) throws IOException {
        AuthSwitchResponsePacket responsePacket = new AuthSwitchResponsePacket();
        responsePacket.authData = new byte[] { 2 };
        byte[] auth = responsePacket.toBytes();
        HeaderPacket h = new HeaderPacket();
        h.setPacketBodyLength(auth.length);
        h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));
        PacketManager.writePkg(channel, h.toBytes(), auth);
        logger.info("caching sha2 password fullAuth request public key packet is sent out.");

        header = PacketManager.readHeader(channel, 4);
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength(), timeout);
        AuthSwitchRequestMoreData packet = new AuthSwitchRequestMoreData();
        packet.fromBytes(body);
        if (packet.status != 0x01) {
            throw new IOException("caching_sha2_password get public key failed");
        }

        logger.info("caching sha2 password fullAuth get server public key succeed.");
        byte[] publicKeyBytes = packet.authData;
        byte[] encryptedPassword = null;
        try {
            encryptedPassword = MySQLPasswordEncrypter.scrambleRsa(publicKeyBytes, pass, seed);
        } catch (Exception e) {
            logger.error("rsa encrypt failed {}", publicKeyBytes);
            throw new IOException("caching_sha2_password auth failed", e);
        }

        // send auth
        responsePacket = new AuthSwitchResponsePacket();
        responsePacket.authData = encryptedPassword;
        auth = responsePacket.toBytes();
        h = new HeaderPacket();
        h.setPacketBodyLength(auth.length);
        h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));
        PacketManager.writePkg(channel, h.toBytes(), auth);
        logger.info("caching sha2 password fullAuth response auth data packet is sent out.");
        return PacketManager.readHeader(channel, 4);
    }

    private HeaderPacket authSwitchAfterAuth(byte[] encryptedPassword, HeaderPacket header) throws IOException {
        assert encryptedPassword != null;
        AuthSwitchResponsePacket responsePacket = new AuthSwitchResponsePacket();
        responsePacket.authData = encryptedPassword;
        byte[] auth = responsePacket.toBytes();

        HeaderPacket h = new HeaderPacket();
        h.setPacketBodyLength(auth.length);
        h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));
        PacketManager.writePkg(channel, h.toBytes(), auth);
        logger.info("auth switch response packet is sent out.");
        header = PacketManager.readHeader(channel, 4);
        return header;
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
                throw new IOException("Unexpected packet with field_count=" + body[0]);
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

    public void setPassword(String password) {
        this.password = password;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public SslInfo getSslInfo() {
        return sslInfo;
    }

    public void setSslInfo(SslInfo sslInfo) {
        this.sslInfo = sslInfo;
    }
}

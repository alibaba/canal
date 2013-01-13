package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.client.ClientAuthenticationPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.server.ErrorPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.server.HandshakeInitializationPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.networking.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.utils.MysqlQueryExecutor;
import com.alibaba.otter.canal.parse.inbound.mysql.utils.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.support.PacketManager;

public class MysqlConnection implements ErosaConnection {

    private static final Logger logger            = LoggerFactory.getLogger(MysqlConnection.class);

    private InetSocketAddress   address;
    private String              username;
    private String              password;
    private long                slaveId;
    private byte                charsetNumber     = 28;
    private Charset             charset           = Charset.forName("UTF-8");
    private String              defaultSchema     = "retl";
    private int                 soTimeout         = 30 * 1000;
    private int                 receiveBufferSize = 16 * 1024;
    private int                 sendBufferSize    = 16 * 1024;

    private SocketChannel       channel;
    private AtomicBoolean       connected         = new AtomicBoolean(false);

    public MysqlConnection(long slaveId){
        this.slaveId = slaveId;
    }

    public MysqlConnection(long slaveId, InetSocketAddress address, String username, String password){
        this(slaveId);

        this.address = address;
        this.username = username;
        this.password = password;
    }

    public MysqlConnection(long slaveId, InetSocketAddress address, String username, String password, Charset charset,
                           String defaultSchema){
        this(slaveId, address, username, password);

        this.charset = charset;
        this.defaultSchema = defaultSchema;
    }

    public void connect() throws IOException {
        if (connected.compareAndSet(false, true)) {
            channel = SocketChannel.open();
            try {
                configChannel(channel);
                logger.info("connect MysqlConnection to {}...", address);
                channel.connect(address);
                negotiate(channel);
                checkSettings(channel);
            } catch (Exception e) {
                logger.warn("connect failed!" + ExceptionUtils.getStackTrace(e));
                disconnect();
                connected.compareAndSet(true, false);
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
                throw new IOException("disconnect " + this.address + " failure:" + ExceptionUtils.getStackTrace(e));
            }
        } else {
            logger.info("the channel {} is not connected", this.address);
        }
    }

    public boolean isConnected() {
        return this.channel != null && this.channel.isConnected();
    }

    public ResultSetPacket query(String cmd) throws IOException {
        checkConnected();

        MysqlQueryExecutor executor = new MysqlQueryExecutor(channel);
        return executor.query(cmd);
    }

    public void update(String cmd) throws IOException {
        checkConnected();

        MysqlUpdateExecutor executor = new MysqlUpdateExecutor(channel);
        executor.update(cmd);
    }

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = this.slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP with position:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(channel, new ByteBuffer[] { ByteBuffer.wrap(binlogDumpHeader.toBytes()),
                ByteBuffer.wrap(cmdBody) });

        byte[] data;
        do {
            data = readEntry();
        } while (func.sink(data));
    }

    @Override
    public void dump(long timestamp, SinkFunction func) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    public MysqlConnection fork() {
        MysqlConnection connection = new MysqlConnection(this.slaveId);
        connection.setCharset(getCharset());
        connection.setCharsetNumber(getCharsetNumber());
        connection.setDefaultSchema(getDefaultSchema());
        connection.setAddress(getAddress());
        connection.setPassword(getPassword());
        connection.setUsername(getUsername());
        connection.setReceiveBufferSize(getReceiveBufferSize());
        connection.setSendBufferSize(getSendBufferSize());
        connection.setSoTimeout(getSoTimeout());
        return connection;
    }

    // ====================== help method ====================

    private void checkConnected() throws IOException {
        if (!connected.get()) {
            throw new IOException("should execute connect first");
        }
    }

    private void configChannel(SocketChannel channel) throws IOException {
        channel.socket().setKeepAlive(true);
        channel.socket().setReuseAddress(true);
        channel.socket().setSoTimeout(soTimeout);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setReceiveBufferSize(receiveBufferSize);
        channel.socket().setSendBufferSize(sendBufferSize);
    }

    private void negotiate(SocketChannel channel) throws IOException {
        HeaderPacket header = PacketManager.readHeader(channel, 4);
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength());
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

        logger.info("handshake initialization packet received, prepare the client authentication packet to send");

        ClientAuthenticationPacket clientAuth = new ClientAuthenticationPacket();
        clientAuth.setCharsetNumber(charsetNumber);

        clientAuth.setUsername(username);
        clientAuth.setPassword(password);
        clientAuth.setServerCapabilities(handshakePacket.serverCapabilities);
        clientAuth.setDatabaseName(defaultSchema);
        clientAuth.setScrumbleBuff(joinAndCreateScrumbleBuff(handshakePacket));

        byte[] clientAuthPkgBody = clientAuth.toBytes();
        HeaderPacket h = new HeaderPacket();
        h.setPacketBodyLength(clientAuthPkgBody.length);
        h.setPacketSequenceNumber((byte) (header.getPacketSequenceNumber() + 1));

        PacketManager.write(channel, new ByteBuffer[] { ByteBuffer.wrap(h.toBytes()),
                ByteBuffer.wrap(clientAuthPkgBody) });
        logger.info("client authentication packet is sent out.");

        // check auth result
        header = null;
        header = PacketManager.readHeader(channel, 4);
        body = null;
        body = PacketManager.readBytes(channel, header.getPacketBodyLength());
        assert body != null;
        if (body[0] < 0) {
            ErrorPacket err = new ErrorPacket();
            err.fromBytes(body);
            throw new IOException("Error When doing Client Authentication:" + err.toString());
        }
    }

    private byte[] joinAndCreateScrumbleBuff(HandshakeInitializationPacket handshakePacket) throws IOException {
        byte[] dest = new byte[handshakePacket.seed.length + handshakePacket.restOfScrambleBuff.length];
        System.arraycopy(handshakePacket.seed, 0, dest, 0, handshakePacket.seed.length);
        System.arraycopy(handshakePacket.restOfScrambleBuff, 0, dest, handshakePacket.seed.length,
                         handshakePacket.restOfScrambleBuff.length);
        return dest;
    }

    /**
     * the settings that will need to be checked or set:<br>
     * <ol>
     * <li>binlog format</li>
     * <li>wait_timeout</li>
     * <li>net_write_timeout</li>
     * <li>net_read_timeout</li>
     * </ol>
     * 
     * @param channel
     * @throws Exception
     */
    private void checkSettings(SocketChannel channel) throws Exception {
        checkBinlogFormat(channel);

        MysqlUpdateExecutor updateExecutor = new MysqlUpdateExecutor(channel);
        try {
            updateExecutor.update("set wait_timeout=9999999");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }
        try {
            updateExecutor.update("set net_write_timeout=1800");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            updateExecutor.update("set net_read_timeout=1800");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

    }

    /**
     * 判断一下是否采用ROW模式
     */
    private void checkBinlogFormat(SocketChannel channel) throws IOException {
        MysqlQueryExecutor queryExecutor = new MysqlQueryExecutor(channel);
        ResultSetPacket rs = queryExecutor.query("show variables like 'binlog_format'");
        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2
            || !StringUtils.equalsIgnoreCase("row", columnValues.get(1))) {
            logger.warn("unexpected binlog format query result, this may cause unexpected result, so throw exception to request network to io shutdown.");
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }
    }

    private byte[] readEntry() throws IOException {
        HeaderPacket header = PacketManager.readHeader(channel, 4);
        if (header.getPacketBodyLength() < 0) {
            logger.warn("unexpected packet length on body with header bytes:{}", Arrays.toString(header.toBytes()));
        }
        // 读取对应的body
        byte[] body = PacketManager.readBytes(channel, header.getPacketBodyLength());
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket error = new ErrorPacket();
                error.fromBytes(body);
                logger.error("Unexpected Error when processing binlog event:{}", error.toString());
            } else if ((body[0] == -2)) {
                logger.error("duplicate slave Id :" + slaveId);
            } else {
                logger.error("unexpected packet type:{}", body[0]);
            }

            throw new IOException("Error When doing read Entry");
        }

        return ArrayUtils.subarray(body, 1, body.length); // skip field count byte
    }

    // ================== setter / getter ===================

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

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public AtomicBoolean getConnected() {
        return connected;
    }

    public void setConnected(AtomicBoolean connected) {
        this.connected = connected;
    }

    public byte getCharsetNumber() {
        return charsetNumber;
    }

    public void setCharsetNumber(byte charsetNumber) {
        this.charsetNumber = charsetNumber;
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

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }
}

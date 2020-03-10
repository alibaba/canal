package com.alibaba.otter.canal.client.impl;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.client.impl.running.ClientRunningData;
import com.alibaba.otter.canal.client.impl.running.ClientRunningListener;
import com.alibaba.otter.canal.client.impl.running.ClientRunningMonitor;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.protocol.CanalPacket.Ack;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAck;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientAuth;
import com.alibaba.otter.canal.protocol.CanalPacket.ClientRollback;
import com.alibaba.otter.canal.protocol.CanalPacket.Compression;
import com.alibaba.otter.canal.protocol.CanalPacket.Get;
import com.alibaba.otter.canal.protocol.CanalPacket.Handshake;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.CanalPacket.PacketType;
import com.alibaba.otter.canal.protocol.CanalPacket.Sub;
import com.alibaba.otter.canal.protocol.CanalPacket.Unsub;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;

/**
 * 基于{@linkplain CanalServerWithNetty}定义的网络协议接口，对于canal数据进行get/rollback/ack等操作
 *
 * @author jianghang 2012-10-24 下午05:37:20
 * @version 1.0.0
 */
public class SimpleCanalConnector implements CanalConnector {

    private static final Logger  logger                = LoggerFactory.getLogger(SimpleCanalConnector.class);
    private SocketAddress        address;
    private String               username;
    private String               password;
    private int                  soTimeout             = 60000;                                              // milliseconds
    private int                  idleTimeout           = 60 * 60 * 1000;                                     // client和server之间的空闲链接超时的时间,默认为1小时
    private String               filter;                                                                     // 记录上一次的filter提交值,便于自动重试时提交

    private final ByteBuffer     readHeader            = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    private final ByteBuffer     writeHeader           = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
    private SocketChannel        channel;
    private ReadableByteChannel  readableChannel;
    private WritableByteChannel  writableChannel;
    private List<Compression>    supportedCompressions = new ArrayList<Compression>();
    private ClientIdentity       clientIdentity;
    private ClientRunningMonitor runningMonitor;                                                             // 运行控制
    private ZkClientx            zkClientx;
    private BooleanMutex         mutex                 = new BooleanMutex(false);
    private volatile boolean     connected             = false;                                              // 代表connected是否已正常执行，因为有HA，不代表在工作中
    private boolean              rollbackOnConnect     = true;                                               // 是否在connect链接成功后，自动执行rollback操作
    private boolean              rollbackOnDisConnect  = false;                                              // 是否在connect链接成功后，自动执行rollback操作
    private boolean              lazyParseEntry        = false;                                              // 是否自动化解析Entry对象,如果考虑最大化性能可以延后解析
    // 读写数据分别使用不同的锁进行控制，减小锁粒度,读也需要排他锁，并发度容易造成数据包混乱，反序列化失败
    private Object               readDataLock          = new Object();
    private Object               writeDataLock         = new Object();

    private volatile boolean     running               = false;

    public SimpleCanalConnector(SocketAddress address, String username, String password, String destination){
        this(address, username, password, destination, 60000, 60 * 60 * 1000);
    }

    public SimpleCanalConnector(SocketAddress address, String username, String password, String destination,
                                int soTimeout){
        this(address, username, password, destination, soTimeout, 60 * 60 * 1000);
    }

    public SimpleCanalConnector(SocketAddress address, String username, String password, String destination,
                                int soTimeout, int idleTimeout){
        this.address = address;
        this.username = username;
        this.password = password;
        this.soTimeout = soTimeout;
        this.idleTimeout = idleTimeout;
        this.clientIdentity = new ClientIdentity(destination, (short) 1001);
    }

    public void connect() throws CanalClientException {
        if (connected) {
            return;
        }

        if (runningMonitor != null) {
            if (!runningMonitor.isStart()) {
                runningMonitor.start();
            }
        } else {
            waitClientRunning();
            if (!running) {
                return;
            }
            doConnect();
            if (filter != null) { // 如果存在条件，说明是自动切换，基于上一次的条件订阅一次
                subscribe(filter);
            }
            if (rollbackOnConnect) {
                rollback();
            }
        }

        connected = true;
    }

    public void disconnect() throws CanalClientException {
        if (rollbackOnDisConnect && channel.isConnected()) {
            rollback();
        }

        connected = false;
        if (runningMonitor != null) {
            if (runningMonitor.isStart()) {
                runningMonitor.stop();
            }
        } else {
            doDisconnect();
        }
    }

    private InetSocketAddress doConnect() throws CanalClientException {
        try {
            channel = SocketChannel.open();
            channel.socket().setSoTimeout(soTimeout);
            SocketAddress address = getAddress();
            if (address == null) {
                address = getNextAddress();
            }
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
            //
            Handshake handshake = Handshake.parseFrom(p.getBody());
            supportedCompressions.add(handshake.getSupportedCompressions());
            //
            ByteString seed = handshake.getSeeds(); // seed for auth
            String newPasswd = password;
            if (password != null) {
                // encode passwd
                newPasswd = SecurityUtil.byte2HexStr(SecurityUtil.scramble411(password.getBytes(), seed.toByteArray()));
            }

            ClientAuth ca = ClientAuth.newBuilder()
                .setUsername(username != null ? username : "")
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
            if (ackBody.getErrorCode() > 0) {
                throw new CanalClientException("something goes wrong when doing authentication: "
                                               + ackBody.getErrorMessage());
            }

            connected = true;
            return new InetSocketAddress(channel.socket().getLocalAddress(), channel.socket().getLocalPort());
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new CanalClientException(e);
        }
    }

    private void doDisconnect() throws CanalClientException {
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

    private void quietlyClose(Channel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("exception on closing channel:{} \n {}", channel, e);
        }
    }

    public void subscribe() throws CanalClientException {
        subscribe(""); // 传递空字符即可
    }

    public void subscribe(String filter) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return;
        }
        try {
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.SUBSCRIPTION)
                .setBody(Sub.newBuilder()
                    .setDestination(clientIdentity.getDestination())
                    .setClientId(String.valueOf(clientIdentity.getClientId()))
                    .setFilter(filter != null ? filter : "")
                    .build()
                    .toByteString())
                .build()
                .toByteArray());
            //
            Packet p = Packet.parseFrom(readNextPacket());
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new CanalClientException("failed to subscribe with reason: " + ack.getErrorMessage());
            }

            clientIdentity.setFilter(filter);
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    public void unsubscribe() throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return;
        }
        try {
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.UNSUBSCRIPTION)
                .setBody(Unsub.newBuilder()
                    .setDestination(clientIdentity.getDestination())
                    .setClientId(String.valueOf(clientIdentity.getClientId()))
                    .build()
                    .toByteString())
                .build()
                .toByteArray());
            //
            Packet p = Packet.parseFrom(readNextPacket());
            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new CanalClientException("failed to unSubscribe with reason: " + ack.getErrorMessage());
            }
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    public Message get(int batchSize) throws CanalClientException {
        return get(batchSize, null, null);
    }

    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        Message message = getWithoutAck(batchSize, timeout, unit);
        ack(message.getId());
        return message;
    }

    public Message getWithoutAck(int batchSize) throws CanalClientException {
        return getWithoutAck(batchSize, null, null);
    }

    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return null;
        }
        try {
            int size = (batchSize <= 0) ? 1000 : batchSize;
            long time = (timeout == null || timeout < 0) ? -1 : timeout; // -1代表不做timeout控制
            if (unit == null) {
                unit = TimeUnit.MILLISECONDS;
            }

            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.GET)
                .setBody(Get.newBuilder()
                    .setAutoAck(false)
                    .setDestination(clientIdentity.getDestination())
                    .setClientId(String.valueOf(clientIdentity.getClientId()))
                    .setFetchSize(size)
                    .setTimeout(time)
                    .setUnit(unit.ordinal())
                    .build()
                    .toByteString())
                .build()
                .toByteArray());
            return receiveMessages();
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    private Message receiveMessages() throws IOException {
        byte[] data = readNextPacket();
        return CanalMessageDeserializer.deserializer(data, lazyParseEntry);
    }

    public void ack(long batchId) throws CanalClientException {
        waitClientRunning();
        if (!running) {
            return;
        }
        ClientAck ca = ClientAck.newBuilder()
            .setDestination(clientIdentity.getDestination())
            .setClientId(String.valueOf(clientIdentity.getClientId()))
            .setBatchId(batchId)
            .build();
        try {
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.CLIENTACK)
                .setBody(ca.toByteString())
                .build()
                .toByteArray());
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    public void rollback(long batchId) throws CanalClientException {
        waitClientRunning();
        ClientRollback ca = ClientRollback.newBuilder()
            .setDestination(clientIdentity.getDestination())
            .setClientId(String.valueOf(clientIdentity.getClientId()))
            .setBatchId(batchId)
            .build();
        try {
            writeWithHeader(Packet.newBuilder()
                .setType(PacketType.CLIENTROLLBACK)
                .setBody(ca.toByteString())
                .build()
                .toByteArray());
        } catch (IOException e) {
            throw new CanalClientException(e);
        }
    }

    public void rollback() throws CanalClientException {
        waitClientRunning();
        rollback(0);// 0代笔未设置
    }

    // ==================== helper method ====================

    private void writeWithHeader(byte[] body) throws IOException {
        writeWithHeader(writableChannel, body);
    }

    private byte[] readNextPacket() throws IOException {
        return readNextPacket(readableChannel);
    }

    private void writeWithHeader(WritableByteChannel channel, byte[] body) throws IOException {
        synchronized (writeDataLock) {
            writeHeader.clear();
            writeHeader.putInt(body.length);
            writeHeader.flip();
            channel.write(writeHeader);
            channel.write(ByteBuffer.wrap(body));
        }
    }

    private byte[] readNextPacket(ReadableByteChannel channel) throws IOException {
        synchronized (readDataLock) {
            readHeader.clear();
            read(channel, readHeader);
            int bodyLen = readHeader.getInt(0);
            ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen).order(ByteOrder.BIG_ENDIAN);
            read(channel, bodyBuf);
            return bodyBuf.array();
        }
    }

    private void read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int r = channel.read(buffer);
            if (r == -1) {
                throw new IOException("end of stream when reading header");
            }
        }
    }

    private synchronized void initClientRunningMonitor(ClientIdentity clientIdentity) {
        if (zkClientx != null && clientIdentity != null && runningMonitor == null) {
            ClientRunningData clientData = new ClientRunningData();
            clientData.setClientId(clientIdentity.getClientId());
            clientData.setAddress(AddressUtils.getHostIp());

            runningMonitor = new ClientRunningMonitor();
            runningMonitor.setDestination(clientIdentity.getDestination());
            runningMonitor.setZkClient(zkClientx);
            runningMonitor.setClientData(clientData);
            runningMonitor.setListener(new ClientRunningListener() {

                public InetSocketAddress processActiveEnter() {
                    InetSocketAddress address = doConnect();
                    mutex.set(true);
                    if (filter != null) { // 如果存在条件，说明是自动切换，基于上一次的条件订阅一次
                        subscribe(filter);
                    }

                    if (rollbackOnConnect) {
                        rollback();
                    }

                    return address;
                }

                public void processActiveExit() {
                    mutex.set(false);
                    doDisconnect();
                }

            });
        }
    }

    private void waitClientRunning() {
        try {
            if (zkClientx != null) {
                if (!connected) {// 未调用connect
                    throw new CanalClientException("should connect first");
                }

                running = true;
                mutex.get();// 阻塞等待
            } else {
                // 单机模式直接设置为running
                running = true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CanalClientException(e);
        }
    }

    public boolean checkValid() {
        if (zkClientx != null) {
            return mutex.state();
        } else {
            return true;// 默认都放过
        }
    }

    public SocketAddress getNextAddress() {
        return null;
    }

    public SocketAddress getAddress() {
        return address;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public void setZkClientx(ZkClientx zkClientx) {
        this.zkClientx = zkClientx;
        initClientRunningMonitor(this.clientIdentity);
    }

    public void setRollbackOnConnect(boolean rollbackOnConnect) {
        this.rollbackOnConnect = rollbackOnConnect;
    }

    public void setRollbackOnDisConnect(boolean rollbackOnDisConnect) {
        this.rollbackOnDisConnect = rollbackOnDisConnect;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public boolean isLazyParseEntry() {
        return lazyParseEntry;
    }

    public void setLazyParseEntry(boolean lazyParseEntry) {
        this.lazyParseEntry = lazyParseEntry;
    }

    public void stopRunning() {
        if (running) {
            running = false; // 设置为非running状态
            if (!mutex.state()) {
                mutex.set(true); // 中断阻塞
            }
        }
    }

}

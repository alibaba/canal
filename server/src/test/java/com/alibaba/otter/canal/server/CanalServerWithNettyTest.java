package com.alibaba.otter.canal.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.protocol.E3.Ack;
import com.alibaba.otter.canal.protocol.E3.ClientAck;
import com.alibaba.otter.canal.protocol.E3.ClientAuth;
import com.alibaba.otter.canal.protocol.E3.ClientRollback;
import com.alibaba.otter.canal.protocol.E3.E3Packet;
import com.alibaba.otter.canal.protocol.E3.Get;
import com.alibaba.otter.canal.protocol.E3.Handshake;
import com.alibaba.otter.canal.protocol.E3.Messages;
import com.alibaba.otter.canal.protocol.E3.PacketType;
import com.alibaba.otter.canal.protocol.E3.Sub;
import com.alibaba.otter.canal.protocol.E3.Unsub;
import com.alibaba.otter.canal.server.embeded.CanalServerWithEmbeded;
import com.alibaba.otter.canal.server.netty.CanalServerWithNetty;
import com.alibaba.otter.shared.common.model.canal.Canal;
import com.alibaba.otter.shared.common.model.canal.CanalParameter;
import com.alibaba.otter.shared.common.model.canal.CanalParameter.HAMode;
import com.alibaba.otter.shared.common.model.canal.CanalParameter.IndexMode;
import com.alibaba.otter.shared.common.model.canal.CanalParameter.MetaMode;
import com.alibaba.otter.shared.common.model.canal.CanalParameter.SourcingType;
import com.alibaba.otter.shared.common.model.canal.CanalParameter.StorageMode;

public class CanalServerWithNettyTest {

    protected static final String cluster1      = "10.20.153.52:2188";
    protected static final String DESTINATION   = "ljhtest1";
    protected static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    protected static final String MYSQL_ADDRESS = "10.20.153.51";
    protected static final String USERNAME      = "retl";
    protected static final String PASSWORD      = "retl";
    protected static final String FILTER        = "retl\\..*,erosa.canaltable1s,erosa.canaltable1t";

    private final ByteBuffer      header        = ByteBuffer.allocate(4);
    private CanalServerWithNetty  nettyServer;

    @Before
    public void setUp() {
        CanalServerWithEmbeded embededServer = new CanalServerWithEmbeded();
        embededServer.setCanalInstanceGenerator(new CanalInstanceGenerator() {

            public CanalInstance generate(String destination) {
                Canal canal = buildCanal();
                return new CanalInstanceWithManager(canal, FILTER);
            }
        });

        nettyServer = new CanalServerWithNetty(embededServer);
        nettyServer.setPort(1088);
        nettyServer.start();
    }

    @Test
    public void testAuth() {
        try {
            SocketChannel channel = SocketChannel.open();
            channel.connect(new InetSocketAddress("127.0.0.1", 1088));
            E3Packet p = E3Packet.parseFrom(readNextPacket(channel));

            if (p.getVersion() != 1) {
                throw new Exception("unsupported version at this client.");
            }

            if (p.getType() != PacketType.HANDSHAKE) {
                throw new Exception("expect handshake but found other type.");
            }
            //
            Handshake handshake = Handshake.parseFrom(p.getBody());
            System.out.println(handshake.getSupportedCompressionsList());
            //
            ClientAuth ca = ClientAuth.newBuilder().setUsername("").setNetReadTimeout(10000).setNetWriteTimeout(10000).build();
            writeWithHeader(
                            channel,
                            E3Packet.newBuilder().setType(PacketType.CLIENTAUTHENTICATION).setBody(ca.toByteString()).build().toByteArray());
            //
            p = E3Packet.parseFrom(readNextPacket(channel));
            if (p.getType() != PacketType.ACK) {
                throw new Exception("unexpected packet type when ack is expected");
            }

            Ack ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new Exception("something goes wrong when doing authentication: " + ack.getErrorMessage());
            }

            writeWithHeader(
                            channel,
                            E3Packet.newBuilder().setType(PacketType.SUBSCRIPTION).setBody(
                                                                                           Sub.newBuilder().setDestination(
                                                                                                                           DESTINATION).setClientId(
                                                                                                                                                    "1").build().toByteString()).build().toByteArray());
            //
            p = E3Packet.parseFrom(readNextPacket(channel));
            ack = Ack.parseFrom(p.getBody());
            if (ack.getErrorCode() > 0) {
                throw new Exception("failed to subscribe with reason: " + ack.getErrorMessage());
            }

            for (int i = 0; i < 10; i++) {
                writeWithHeader(
                                channel,
                                E3Packet.newBuilder().setType(PacketType.GET).setBody(
                                                                                      Get.newBuilder().setDestination(
                                                                                                                      DESTINATION).setClientId(
                                                                                                                                               "1").setFetchSize(
                                                                                                                                                                 10).build().toByteString()).build().toByteArray());
                p = E3Packet.parseFrom(readNextPacket(channel));

                long batchId = -1L;
                switch (p.getType()) {
                    case MESSAGES: {
                        Messages messages = Messages.parseFrom(p.getBody());
                        batchId = messages.getBatchId();
                        break;
                    }
                    case ACK: {
                        ack = Ack.parseFrom(p.getBody());
                        if (ack.getErrorCode() > 0) {
                            throw new Exception("failed to subscribe with reason: " + ack.getErrorMessage());
                        }
                        break;
                    }
                    default: {
                        throw new Exception("unexpected packet type: " + p.getType());
                    }
                }

                System.out.println("!!!!!!!!!!!!!!!!! " + batchId);
                Thread.sleep(1000L);
                writeWithHeader(
                                channel,
                                E3Packet.newBuilder().setType(PacketType.CLIENTACK).setBody(
                                                                                            ClientAck.newBuilder().setDestination(
                                                                                                                                  DESTINATION).setClientId(
                                                                                                                                                           "1").setBatchId(
                                                                                                                                                                           batchId).build().toByteString()).build().toByteArray());
            }

            writeWithHeader(
                            channel,
                            E3Packet.newBuilder().setType(PacketType.CLIENTROLLBACK).setBody(
                                                                                             ClientRollback.newBuilder().setDestination(
                                                                                                                                        DESTINATION).setClientId(
                                                                                                                                                                 "1").build().toByteString()).build().toByteArray());

            writeWithHeader(
                            channel,
                            E3Packet.newBuilder().setType(PacketType.UNSUBSCRIPTION).setBody(
                                                                                             Unsub.newBuilder().setDestination(
                                                                                                                               DESTINATION).setClientId(
                                                                                                                                                        "1").build().toByteString()).build().toByteArray());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        nettyServer.stop();
    }

    private byte[] readNextPacket(SocketChannel channel) throws IOException {
        header.clear();
        read(channel, header);
        int bodyLen = header.getInt(0);
        ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen);
        read(channel, bodyBuf);
        return bodyBuf.array();
    }

    private void writeWithHeader(SocketChannel channel, byte[] body) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(4);
        header.putInt(body.length);
        header.flip();
        int len = channel.write(header);
        assert (len == header.capacity());

        channel.write(ByteBuffer.wrap(body));
    }

    private void read(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int r = channel.read(buffer);
            if (r == -1) {
                throw new IOException("end of stream when reading header");
            }
        }
    }

    private Canal buildCanal() {
        Canal canal = new Canal();
        canal.setId(1L);
        canal.setName(DESTINATION);
        canal.setDesc("test");

        CanalParameter parameter = new CanalParameter();

        parameter.setZkClusters(Arrays.asList("10.20.153.52:2188"));
        parameter.setMetaMode(MetaMode.MEMORY);
        parameter.setHaMode(HAMode.HEARTBEAT);
        parameter.setIndexMode(IndexMode.MEMORY);

        parameter.setStorageMode(StorageMode.MEMORY);
        parameter.setMemoryStorageBufferSize(32 * 1024);

        parameter.setSourcingType(SourcingType.MYSQL);
        parameter.setDbAddresses(Arrays.asList(new InetSocketAddress(MYSQL_ADDRESS, 3306),
                                               new InetSocketAddress(MYSQL_ADDRESS, 3306)));
        parameter.setDbUsername(USERNAME);
        parameter.setDbPassword(PASSWORD);
        parameter.setPositions(Arrays.asList(
                                             "{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}",
                                             "{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}"));

        parameter.setSlaveId(1234L);

        parameter.setDefaultConnectionTimeoutInSeconds(30);
        parameter.setConnectionCharset("UTF-8");
        parameter.setConnectionCharsetNumber((byte) 33);
        parameter.setReceiveBufferSize(8 * 1024);
        parameter.setSendBufferSize(8 * 1024);

        parameter.setDetectingEnable(false);
        parameter.setDetectingIntervalInSeconds(10);
        parameter.setDetectingRetryTimes(3);
        parameter.setDetectingSQL(DETECTING_SQL);

        canal.setCanalParameter(parameter);
        return canal;
    }
}

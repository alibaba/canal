package com.alibaba.otter.canal.parse.inbound.emulator.oracle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.alibaba.otter.canal.parse.inbound.emulator.oracle.data.BinLogFile;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.CanalPacket.Packet;
import com.alibaba.otter.canal.protocol.CanalPacket.PacketType;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Oracle Erosa Server Handler
 * 
 * @author: yuanzu Date: 12-9-24 Time: 下午3:00
 */
public class OracleErosaServerHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger           = Logger.getLogger(OracleErosaServerHandler.class.getName());
    private final BinLogFile[]  files;
    private int                 binLogFileIndex  = -1;
    private long                binLogFileOffset = 0;

    private RunningStatus       status           = RunningStatus.HandShake;

    public OracleErosaServerHandler(BinLogFile[] files){
        this.files = files;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        super.channelConnected(ctx, e);

        if (this.status != RunningStatus.HandShake) {
            logger.log(Level.WARNING, "expect handshake status, got " + this.status);
            e.getChannel().close();
            return;
        }

        sendPacket(e.getChannel(), CanalPacket.Packet.newBuilder().setType(CanalPacket.PacketType.HANDSHAKE).build());

        this.status = RunningStatus.ClientAuth;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        try {
            Object message = e.getMessage();
            if (message instanceof CanalPacket.Packet) {
                CanalPacket.Packet e3Packet = (CanalPacket.Packet) message;
                switch (this.status) {
                    case ClientAuth:
                        if (!authenticate(CanalPacket.ClientAuth.parseFrom(e3Packet.getBody()))) {
                            logger.log(Level.WARNING, "user auth failed");
                            e.getChannel().close();
                            return;
                        }

                        sendPacket(e.getChannel(),
                                   CanalPacket.Packet.newBuilder().setType(CanalPacket.PacketType.ACK).build());

                        this.status = RunningStatus.Dump;
                        break;
                    case Dump:
                        if (!dump(CanalPacket.Dump.parseFrom(e3Packet.getBody()))) {
                            e.getChannel().close();
                            return;
                        }

                        this.status = RunningStatus.Serving;

                        // send entries
                        // noinspection InfiniteLoopStatement
                        while (true) {
                            sendEntries(e.getChannel());

                            Thread.sleep(1000);
                        }
                    default:
                        break;
                }
            }
        } catch (Exception ex) {
            e.getChannel().close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
        e.getChannel().close();
    }

    private void sendEntries(Channel channel) throws IOException {
        for (; this.binLogFileIndex < this.files.length; this.binLogFileIndex++) {
            BinLogFile binLogFile = this.files[this.binLogFileIndex];

            while (this.binLogFileOffset < binLogFile.getLength()) {
                ChannelBuffer bufLen = ChannelBuffers.wrappedBuffer(binLogFile.getData((int) this.binLogFileOffset, 4));
                bufLen.markReaderIndex();
                int dataLen = bufLen.readInt();
                bufLen.resetReaderIndex();

                byte[] data = binLogFile.getData((int) this.binLogFileOffset + 4, dataLen);
                CanalEntry.Entry oldEntry = CanalEntry.Entry.parseFrom(data);

                // 替换为正确的log file name和log file offset
                CanalEntry.Entry newEntry = CanalEntry.Entry.newBuilder(oldEntry).setHeader(CanalEntry.Header.newBuilder(oldEntry.getHeader()).setLogfileName(binLogFile.getFilename()).setLogfileOffset(this.binLogFileOffset).build()).build();
                Packet packet = Packet.newBuilder().setBody(newEntry.toByteString()).setType(PacketType.MESSAGES).build();

                bufLen.resetWriterIndex();
                bufLen.writeInt(packet.getSerializedSize());
                bufLen.resetReaderIndex();

                ChannelBuffer bufData = ChannelBuffers.wrappedBuffer(packet.toByteArray());

                channel.write(ChannelBuffers.wrappedBuffer(bufLen, bufData));

                this.binLogFileOffset += (4 + dataLen);
            }
            this.binLogFileOffset = 0;
        }
    }

    /**
     * fixed username/password: erosa_user1/12345
     * 
     * @param clientAuth client auth packet
     * @return true if matched
     */
    private boolean authenticate(CanalPacket.ClientAuth clientAuth) {
        return StringUtils.equals(clientAuth.getUsername(), "erosa_user1")
               && StringUtils.equals(clientAuth.getPassword().toStringUtf8(), "12345");
    }

    private boolean dump(CanalPacket.Dump dump) throws InvalidProtocolBufferException {
        String journalName = dump.getJournal();
        long position = dump.getPosition();
        long timestamp = dump.getTimestamp();

        if (timestamp == 0) {
            // seek by filename & position
            for (int i = 0; i < this.files.length; i++) {
                if (StringUtils.equals(this.files[i].getFilename(), journalName)) {
                    this.binLogFileIndex = i;
                    break;
                }
            }

            if (this.binLogFileIndex == -1) {
                logger.log(Level.WARNING, "invalid journal name : " + journalName);
                return false;
            }

            if (position > this.files[this.binLogFileIndex].getLength()) {
                logger.log(Level.WARNING, "invalid journal position : " + position);
                return false;
            }

            this.binLogFileOffset = position;
        } else {
            this.binLogFileIndex = 0;
            this.binLogFileOffset = 0;

            int offset;
            int index;

            for (int i = 0; i < this.files.length; i++) {
                index = i;
                offset = 0;

                BinLogFile file = this.files[i];

                while (offset < file.getLength()) {
                    ByteBuffer bufLen = ByteBuffer.wrap(file.getData(offset, 4));
                    int len = bufLen.getInt();

                    byte[] data = file.getData(offset + 4, len);
                    CanalEntry.Entry entry = CanalEntry.Entry.parseFrom(data);
                    long executeTime = entry.getHeader().getExecuteTime();

                    offset += (4 + len);

                    if (timestamp > executeTime) {
                        this.binLogFileIndex = index;
                        this.binLogFileOffset = offset;
                    } else {
                        break;
                    }
                }
            }
        }

        return true;
    }

    /**
     * 发送数据包 | 4字节数据包长度 | 数据包字节数组 |
     * 
     * @throws java.io.IOException
     */
    private static boolean sendPacket(Channel channel, CanalPacket.Packet packet) throws IOException {
        int len = packet.getSerializedSize();
        ChannelBuffer bufLen = ChannelBuffers.buffer(4);
        bufLen.writeInt(len);

        channel.write(ChannelBuffers.wrappedBuffer(bufLen, ChannelBuffers.wrappedBuffer(packet.toByteArray())));

        return true;
    }

    enum RunningStatus {
        HandShake, ClientAuth, Dump, Serving
    }
}

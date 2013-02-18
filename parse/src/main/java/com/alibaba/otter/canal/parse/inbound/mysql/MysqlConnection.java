package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlQueryExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;

public class MysqlConnection implements ErosaConnection {

    private static final Logger logger  = LoggerFactory.getLogger(MysqlConnection.class);

    private MysqlConnector      connector;
    private long                slaveId;
    private Charset             charset = Charset.forName("UTF-8");

    public MysqlConnection(){
    }

    public MysqlConnection(InetSocketAddress address, String username, String password){

        connector = new MysqlConnector(address, username, password);
    }

    public MysqlConnection(InetSocketAddress address, String username, String password, byte charsetNumber,
                           String defaultSchema){
        connector = new MysqlConnector(address, username, password, charsetNumber, defaultSchema);
    }

    public void connect() throws IOException {
        connector.connect();
    }

    public void reconnect() throws IOException {
        connector.reconnect();
    }

    public void disconnect() throws IOException {
        connector.disconnect();
    }

    public boolean isConnected() {
        return connector.isConnected();
    }

    public ResultSetPacket query(String cmd) throws IOException {
        MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
        return exector.query(cmd);
    }

    public void update(String cmd) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    /**
     * 加速主备切换时的查找速度，做一些特殊优化，比如只解析事务头或者尾
     */
    public void seek(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        checkBinlogFormat();
        updateSettings();

        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        while (fetcher.fetch()) {
            LogEvent event = null;
            event = decoder.decode(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        checkBinlogFormat();
        updateSettings();

        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        while (fetcher.fetch()) {
            LogEvent event = null;
            event = decoder.decode(fetcher, context);

            if (event == null) {
                throw new CanalParseException("parse failed");
            }

            if (!func.sink(event)) {
                break;
            }
        }
    }

    public void dump(long timestamp, SinkFunction func) throws IOException {
        throw new NullPointerException("Not implement yet");
    }

    private void sendBinlogDump(String binlogfilename, Long binlogPosition) throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = this.slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP with position:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(connector.getChannel(), new ByteBuffer[] { ByteBuffer.wrap(binlogDumpHeader.toBytes()),
                ByteBuffer.wrap(cmdBody) });
    }

    public MysqlConnection fork() {
        MysqlConnection connection = new MysqlConnection();
        connection.setCharset(getCharset());
        connection.setSlaveId(getSlaveId());
        connection.setConnector(connector.fork());
        return connection;
    }

    // ====================== help method ====================

    /**
     * the settings that will need to be checked or set:<br>
     * <ol>
     * <li>wait_timeout</li>
     * <li>net_write_timeout</li>
     * <li>net_read_timeout</li>
     * </ol>
     * 
     * @param channel
     * @throws IOException
     */
    private void updateSettings() throws IOException {
        try {
            update("set wait_timeout=9999999");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }
        try {
            update("set net_write_timeout=1800");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            update("set net_read_timeout=1800");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

        try {
            // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
            update("set names 'binary'");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }

    }

    /**
     * 判断一下是否采用ROW模式
     */
    private void checkBinlogFormat() throws IOException {
        ResultSetPacket rs = query("show variables like 'binlog_format'");
        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2
            || !StringUtils.equalsIgnoreCase("row", columnValues.get(1))) {
            logger.warn("unexpected binlog format query result, this may cause unexpected result, so throw exception to request network to io shutdown.");
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }
    }

    /**
     * private byte[] readEntry() throws IOException { HeaderPacket header = PacketManager.readHeader(channel, 4); if
     * (header.getPacketBodyLength() < 0) { logger.warn("unexpected packet length on body with header bytes:{}",
     * Arrays.toString(header.toBytes())); } // 读取对应的body byte[] body = PacketManager.readBytes(channel,
     * header.getPacketBodyLength()); if (body[0] < 0) { if (body[0] == -1) { ErrorPacket error = new ErrorPacket();
     * error.fromBytes(body); logger.error("Unexpected Error when processing binlog event:{}", error.toString()); } else
     * if ((body[0] == -2)) { logger.error("duplicate slave Id :" + slaveId); } else {
     * logger.error("unexpected packet type:{}", body[0]); } throw new IOException("Error When doing read Entry"); }
     * return ArrayUtils.subarray(body, 1, body.length); // skip field count byte }
     **/

    // ================== setter / getter ===================

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public MysqlConnector getConnector() {
        return connector;
    }

    public void setConnector(MysqlConnector connector) {
        this.connector = connector;
    }

}

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
import com.taobao.tddl.dbsync.binlog.LogPosition;
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent;

public class MysqlConnection implements ErosaConnection {

    private static final Logger logger  = LoggerFactory.getLogger(MysqlConnection.class);

    private MysqlConnector      connector;
    private long                slaveId;
    private Charset             charset = Charset.forName("UTF-8");
    private BinlogFormat        binlogFormat;
    private BinlogImage         binlogImage;
    private int                 binlogChecksum;

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
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);
        LogContext context = new LogContext();
        context.setLogPosition(new LogPosition(binlogfilename));
        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
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
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDump(binlogfilename, binlogPosition);
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        context.setLogPosition(new LogPosition(binlogfilename));
        context.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
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

        connector.setDumping(true);
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

        try {
            // mysql5.6针对checksum支持需要设置session变量
            // 如果不设置会出现错误： Slave can not handle replication events with the
            // checksum that master is configured to log
            // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
            // '@@global.binlog_checksum'需要去掉单引号,在mysql 5.6.29下导致master退出
            update("set @master_binlog_checksum= @@global.binlog_checksum");
        } catch (Exception e) {
            if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
                logger.warn(ExceptionUtils.getFullStackTrace(e));
            }
        }

        try {
            // 参考:https://github.com/alibaba/canal/issues/284
            // mysql5.6需要设置slave_uuid避免被server kill链接
            update("set @slave_uuid=uuid()");
        } catch (Exception e) {
            if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
                logger.warn(ExceptionUtils.getFullStackTrace(e));
            }
        }

        try {
            // mariadb针对特殊的类型，需要设置session变量
            update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getFullStackTrace(e));
        }
    }

    /**
     * 获取一下binlog format格式
     */
    private void loadBinlogFormat() {
        ResultSetPacket rs = null;
        try {
            rs = query("show variables like 'binlog_format'");
        } catch (IOException e) {
            throw new CanalParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            logger.warn("unexpected binlog format query result, this may cause unexpected result, so throw exception to request network to io shutdown.");
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }

        binlogFormat = BinlogFormat.valuesOf(columnValues.get(1));
        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }
    }

    /**
     * 获取一下binlog image格式
     */
    private void loadBinlogImage() {
        ResultSetPacket rs = null;
        try {
            rs = query("show variables like 'binlog_row_image'");
        } catch (IOException e) {
            throw new CanalParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            // 可能历时版本没有image特性
            binlogImage = BinlogImage.FULL;
        } else {
            binlogImage = BinlogImage.valuesOf(columnValues.get(1));
        }

        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog image query result:" + rs.getFieldValues());
        }
    }

    /**
     * 获取主库checksum信息
     * https://dev.mysql.com/doc/refman/5.6/en/replication-options
     * -binary-log.html#option_mysqld_binlog-checksum
     */
    private void loadBinlogChecksum() {
        ResultSetPacket rs = null;
        try {
            rs = query("select @master_binlog_checksum");
        } catch (IOException e) {
            throw new CanalParseException(e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues != null && columnValues.size() >= 1 && columnValues.get(0) != null
            && columnValues.get(0).toUpperCase().equals("CRC32")) {
            binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_CRC32;
        } else {
            binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_OFF;
        }
    }

    public static enum BinlogFormat {

        STATEMENT("STATEMENT"), ROW("ROW"), MIXED("MIXED");

        public boolean isStatement() {
            return this == STATEMENT;
        }

        public boolean isRow() {
            return this == ROW;
        }

        public boolean isMixed() {
            return this == MIXED;
        }

        private String value;

        private BinlogFormat(String value){
            this.value = value;
        }

        public static BinlogFormat valuesOf(String value) {
            BinlogFormat[] formats = values();
            for (BinlogFormat format : formats) {
                if (format.value.equalsIgnoreCase(value)) {
                    return format;
                }
            }
            return null;
        }
    }

    /**
     * http://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.
     * html#sysvar_binlog_row_image
     * 
     * @author agapple 2015年6月29日 下午10:39:03
     * @since 1.0.20
     */
    public static enum BinlogImage {

        FULL("FULL"), MINIMAL("MINIMAL"), NOBLOB("NOBLOB");

        public boolean isFull() {
            return this == FULL;
        }

        public boolean isMinimal() {
            return this == MINIMAL;
        }

        public boolean isNoBlob() {
            return this == NOBLOB;
        }

        private String value;

        private BinlogImage(String value){
            this.value = value;
        }

        public static BinlogImage valuesOf(String value) {
            BinlogImage[] formats = values();
            for (BinlogImage format : formats) {
                if (format.value.equalsIgnoreCase(value)) {
                    return format;
                }
            }
            return null;
        }
    }

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

    public BinlogFormat getBinlogFormat() {
        if (binlogFormat == null) {
            synchronized (this) {
                loadBinlogFormat();
            }
        }

        return binlogFormat;
    }

    public BinlogImage getBinlogImage() {
        if (binlogImage == null) {
            synchronized (this) {
                loadBinlogImage();
            }
        }

        return binlogImage;
    }

}

package com.alibaba.otter.canal.parse.inbound.mysql;

import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;

public abstract class AbstractMysqlEventParser extends AbstractEventParser {

    protected final Logger      logger                  = LoggerFactory.getLogger(this.getClass());
    protected static final long BINLOG_START_OFFEST     = 4L;

    // 编码信息
    protected byte              connectionCharsetNumber = (byte) 33;
    protected Charset           connectionCharset       = Charset.forName("UTF-8");

    protected BinlogParser buildParser() {
        BinlogParser parser = new LogEventConvert();
        setBinlogParser(parser);
        return parser;
    }

    // ============================ setter / getter =========================

    public void setConnectionCharsetNumber(byte connectionCharsetNumber) {
        this.connectionCharsetNumber = connectionCharsetNumber;
    }

    public void setConnectionCharset(Charset connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

}

package com.alibaba.otter.canal.parse.inbound.mysql;

import java.nio.charset.Charset;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.erosa.parse.DefaultMysqlBinlogParser;
import com.alibaba.erosa.parse.MysqlBinlogParser;
import com.alibaba.erosa.protocol.protobuf.ErosaEntry.Entry;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractBinlogParser;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.google.protobuf.TextFormat;

public abstract class AbstractMysqlEventParser extends AbstractEventParser {

    protected final Logger      logger                  = LoggerFactory.getLogger(this.getClass());
    protected static final long BINLOG_START_OFFEST     = 4L;

    // 编码信息
    protected byte              connectionCharsetNumber = (byte) 33;
    protected Charset           connectionCharset       = Charset.forName("UTF-8");

    protected BinlogParser buildParser() {
        BinlogParser parser = new AbstractBinlogParser() {

            final MysqlBinlogParser parser = new DefaultMysqlBinlogParser(connectionCharset, runningInfo.getAddress(),
                                                                          runningInfo.getUsername(),
                                                                          runningInfo.getPassword());

            @Override
            public List<Entry> parse(byte[] event) throws CanalParseException {
                try {
                    return parser.parse(event);
                } catch (TextFormat.ParseException e) {
                    throw new CanalParseException(e);
                }
            }

            public void reset() {
                parser.dispose();
            }

        };

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

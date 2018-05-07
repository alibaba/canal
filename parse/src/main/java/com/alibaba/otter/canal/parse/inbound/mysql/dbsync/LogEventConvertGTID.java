package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hiwjd on 2018/5/2.
 * hiwjd0@gmail.com
 */
public class LogEventConvertGTID extends LogEventConvert implements BinlogParser<LogEvent> {

    public static final Logger logger = LoggerFactory.getLogger(LogEventConvertGTID.class);

    // latest gtid
    private GTIDSet gtidSet;

    public LogEventConvertGTID(GTIDSet gtidSet) {
        this.gtidSet = gtidSet;
    }

    @Override
    public CanalEntry.Entry parse(LogEvent event, boolean isSeek) throws CanalParseException {
        CanalEntry.Entry entry = super.parse(event, isSeek);
        if (entry == null) {
            return null;
        }

        if (entry.getEntryType() == CanalEntry.EntryType.GTIDLOG) {
            try {
                CanalEntry.Pair pair = CanalEntry.Pair.parseFrom(entry.getStoreValue());
                gtidSet.update(pair.getValue());

            } catch (InvalidProtocolBufferException e) {
                logger.error("retrive gtid failed.", e);
                throw new CanalParseException("retrive gtid failed.", e);
            }

            return null;
        }

        if (gtidSet != null) {
            String gtid = gtidSet.toString();
            CanalEntry.Header header = entry.getHeader().toBuilder().setGtid(gtid).build();
            entry = entry.toBuilder().setHeader(header).build();
        }

        return entry;
    }
}

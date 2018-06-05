package com.alibaba.otter.canal.parse.inbound.mongo;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.BinlogParser;
import com.alibaba.otter.canal.parse.inbound.mongo.meta.OpLogMeta;
import com.alibaba.otter.canal.parse.inbound.mongo.meta.OpLogOperation;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * oplog解析器
 * @author dsqin
 * @date 2018/5/16
 */
public class OpLogParser extends AbstractCanalLifeCycle implements BinlogParser<OpLogMeta> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public final static int VERSION = 1;

    public final static String UTF8 = "utf-8";

    @Override
    public CanalEntry.Entry parse(OpLogMeta event, boolean isSeek) throws CanalParseException {

        if (null == event) {
            return null;
        }

        OpLogOperation opLogOperation = event.getOperation();

        CanalEntry.Entry entry = null;

        switch (opLogOperation) {
            case Insert:
                entry = parseInsertEvent(event, isSeek);
                break;
            case Update:
                entry = parseUpdateEvent(event, isSeek);
                break;
            case Delete:
                entry = parseDeleteEvent(event, isSeek);
                break;
        }

        return entry;
    }

    private CanalEntry.Entry parseInsertEvent(OpLogMeta opLogMeta, boolean isSeek) {

        CanalEntry.Header header = createHeader(opLogMeta, CanalEntry.EventType.INSERT);
        ByteString byteString = ByteString.copyFromUtf8(opLogMeta.getData());
        return createEntry(header, CanalEntry.EntryType.ROWDATA_CUSTORM_MONGO, byteString);
    }

    private CanalEntry.Entry parseUpdateEvent(OpLogMeta opLogMeta, boolean isSeek) {

        CanalEntry.Header header = createHeader(opLogMeta, CanalEntry.EventType.UPDATE);
        ByteString byteString = ByteString.copyFromUtf8(opLogMeta.getData());
        return createEntry(header, CanalEntry.EntryType.ROWDATA_CUSTORM_MONGO, byteString);
    }

    private CanalEntry.Entry parseDeleteEvent(OpLogMeta opLogMeta, boolean isSeek) {

        CanalEntry.Header header = createHeader(opLogMeta, CanalEntry.EventType.DELETE);
        ByteString byteString = ByteString.copyFromUtf8(opLogMeta.getData());
        return createEntry(header, CanalEntry.EntryType.ROWDATA_CUSTORM_MONGO, byteString);
    }


    private static CanalEntry.Header createHeader(OpLogMeta opLogMeta, CanalEntry.EventType eventType) {

        long sequence = opLogMeta.getSequence();
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setVersion(VERSION);
        headerBuilder.setServerId(sequence);
        headerBuilder.setServerenCode(UTF8);// 经过java输出后所有的编码为unicode
        headerBuilder.setExecuteTime(System.currentTimeMillis());
        headerBuilder.setSourceType(CanalEntry.Type.MONGO);
        headerBuilder.setEventType(eventType);
        headerBuilder.setLogfileOffset(opLogMeta.getTimestamp());

        ByteString schemaNameByteString = ByteString.copyFromUtf8(opLogMeta.getNameSpace());
        headerBuilder.setSchemaNameBytes(schemaNameByteString);

        return headerBuilder.build();
    }

    public static CanalEntry.Entry createEntry(CanalEntry.Header header, CanalEntry.EntryType entryType, ByteString storeValue) {

        CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        entryBuilder.setStoreValue(storeValue);
        return entryBuilder.build();
    }

    @Override
    public void reset() {
    }

    @Override
    public void stop() {
        reset();
        super.stop();
    }
}

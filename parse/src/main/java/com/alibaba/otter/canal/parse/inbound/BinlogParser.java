package com.alibaba.otter.canal.parse.inbound;

import java.util.List;

import com.alibaba.erosa.protocol.protobuf.ErosaEntry;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;

/**
 * 解析binlog的接口
 * 
 * @author: yuanzu Date: 12-9-20 Time: 下午8:46
 */
public interface BinlogParser extends CanalLifeCycle {

    List<ErosaEntry.Entry> parse(byte[] event) throws CanalParseException;

    void reset();
}

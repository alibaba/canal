package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;

/**
 * 通用的Erosa的链接接口, 用于一般化处理mysql/oracle的解析过程
 * 
 * @author: yuanzu Date: 12-9-20 Time: 下午2:47
 */
public interface ErosaConnection {

    public void connect() throws IOException;

    public void reconnect() throws IOException;

    public void disconnect() throws IOException;

    /**
     * 用于快速数据查找,和dump的区别在于，seek会只给出部分的数据
     */
    public void seek(String binlogfilename, Long binlogPosition, String gtid, SinkFunction func) throws IOException;

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException;

    public void dump(long timestamp, SinkFunction func) throws IOException;

    /**
     * 通过GTID同步binlog
     */
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException;

    // -------------

    public void dump(String binlogfilename, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException;

    public void dump(long timestamp, MultiStageCoprocessor coprocessor) throws IOException;

    public void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException;

    ErosaConnection fork();

    public long queryServerId() throws IOException;
}

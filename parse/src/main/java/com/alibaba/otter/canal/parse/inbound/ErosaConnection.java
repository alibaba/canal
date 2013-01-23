package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;

/**
 * 通用的Erosa的链接接口, 用于一般化处理mysql/oracle的解析过程
 * 
 * @author: yuanzu Date: 12-9-20 Time: 下午2:47
 */
public interface ErosaConnection {

    public void connect() throws IOException;

    public void reconnect() throws IOException;

    public void disconnect() throws IOException;

    public boolean isConnected();

    /**
     * 用于快速数据查找,和dump的区别在于，seek会只给出部分的数据
     */
    public void seek(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException;

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException;

    public void dump(long timestamp, SinkFunction func) throws IOException;

    ErosaConnection fork();
}

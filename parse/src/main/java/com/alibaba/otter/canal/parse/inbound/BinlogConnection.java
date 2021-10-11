package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;

/**
 * Binlog数据源连接
 *
 * @author wanghe Date: 2021/9/8 Time: 16:16
 */
public interface BinlogConnection {

    /**
     * 连接数据源
     *
     * @throws IOException 异常
     */
    void connect() throws IOException;

    /**
     * 重连数据源
     *
     * @throws IOException 异常
     */
    void reconnect() throws IOException;

    /**
     * 断开连接
     *
     * @throws IOException 异常
     */
    void disconnect() throws IOException;

}

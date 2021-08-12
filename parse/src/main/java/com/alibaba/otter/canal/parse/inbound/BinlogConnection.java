package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;

/**
 * @author wanghe
 */
public interface BinlogConnection {

    /**
     * Connect a data source of binlog
     *
     * @throws IOException if exception occurs
     */
    void connect() throws IOException;

    /**
     * Reconnect the data source of binlog
     *
     * @throws IOException if exception occurs
     */
    void reconnect() throws IOException;

    /**
     * Disconnect from the data source of binlog
     *
     * @throws IOException if exception occurs
     */
    void disconnect() throws IOException;

}

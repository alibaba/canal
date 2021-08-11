package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;

public interface BinlogConnection {

    void connect() throws IOException;

    void reconnect() throws IOException;

    void disconnect() throws IOException;

}

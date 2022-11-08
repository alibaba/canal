package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * @author agapple 2018年3月12日 下午10:36:44
 * @since 1.0.26
 */
public interface SocketChannel {

    public void write(byte[]... buf) throws IOException;

    public byte[] read(int readSize) throws IOException;

    public byte[] read(int readSize, int timeout) throws IOException;

    public void read(byte[] data, int off, int len, int timeout) throws IOException;

    public boolean isConnected();

    public SocketAddress getRemoteSocketAddress();

    public SocketAddress getLocalSocketAddress();

    public void close();
}

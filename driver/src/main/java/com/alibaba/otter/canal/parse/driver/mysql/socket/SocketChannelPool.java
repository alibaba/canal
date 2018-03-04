package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.net.Socket;
import java.net.SocketAddress;
import static com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel.DEFAULT_CONNECT_TIMEOUT;
import static com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel.SO_TIMEOUT;

/**
 * @author luoyaogui 实现channel的管理（监听连接、读数据、回收） 2016-12-28
 * @author chuanyi 2018-3-3
 * 保留<code>open</code>减少文件变更数量
 */
public abstract class SocketChannelPool {

    public static SocketChannel open(SocketAddress address) throws Exception {
        Socket socket = new Socket();
        socket.setReceiveBufferSize(32 * 1024);
        socket.setSendBufferSize(32 * 1024);
        socket.setSoTimeout(SO_TIMEOUT);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.setReuseAddress(true);
        socket.connect(address, DEFAULT_CONNECT_TIMEOUT);
        return new SocketChannel(socket);
    }

}
package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.net.Socket;
import java.net.SocketAddress;

/**
 * @author luoyaogui 实现channel的管理（监听连接、读数据、回收） 2016-12-28
 * @author chuanyi 2018-3-3 保留<code>open</code>减少文件变更数量
 */
public abstract class BioSocketChannelPool {

    public static BioSocketChannel open(SocketAddress address) throws Exception {
        Socket socket = new Socket();
        socket.setSoTimeout(BioSocketChannel.SO_TIMEOUT);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.setReuseAddress(true);
        socket.connect(address, BioSocketChannel.DEFAULT_CONNECT_TIMEOUT);
        return new BioSocketChannel(socket);
    }

}

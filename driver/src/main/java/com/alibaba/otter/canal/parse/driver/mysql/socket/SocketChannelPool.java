package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.net.SocketAddress;

import org.apache.commons.lang.StringUtils;

/**
 * @author agapple 2018年3月12日 下午10:46:22
 * @since 1.0.26
 */
public abstract class SocketChannelPool {

    public static SocketChannel open(SocketAddress address) throws Exception {
        String type = chooseSocketChannel();
        if ("netty".equalsIgnoreCase(type)) {
            return NettySocketChannelPool.open(address);
        } else {
            return BioSocketChannelPool.open(address);
        }

    }

    private static String chooseSocketChannel() {
        String socketChannel = System.getenv("canal.socketChannel");
        if (StringUtils.isEmpty(socketChannel)) {
            socketChannel = System.getProperty("canal.socketChannel");
        }

        if (StringUtils.isEmpty(socketChannel)) {
            socketChannel = "bio"; // bio or netty
        }

        return socketChannel;
    }
}

package com.alibaba.otter.canal.parse.driver.mysql.socket;

import java.io.IOException;
import java.net.SocketAddress;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.ssl.SslInfo;
import com.alibaba.otter.canal.parse.driver.mysql.ssl.SslMode;

/**
 * @author agapple 2018年3月12日 下午10:46:22
 * @since 1.0.26
 */
public abstract class SocketChannelPool {

    private static final Logger logger = LoggerFactory.getLogger(SocketChannelPool.class);

    public static SocketChannel open(SocketAddress address) throws Exception {
        String type = chooseSocketChannel();
        if ("netty".equalsIgnoreCase(type)) {
            return NettySocketChannelPool.open(address);
        } else {
            return BioSocketChannelPool.open(address);
        }
    }

    public static SocketChannel connectSsl(SocketChannel channel, SslInfo sslInfo) throws IOException {
        SslMode sslMode = sslInfo.getSslMode();
        String type = chooseSocketChannel();
        if ("netty".equalsIgnoreCase(type)) {
            throw new UnsupportedOperationException("canal socketChannel netty not support ssl mode: " + sslMode);
        } else {
            SocketAddress remoteSocketAddress = channel.getRemoteSocketAddress();
            try {
                return BioSocketChannelPool.openSsl(((BioSocketChannel) channel).getSocket(), sslInfo);
            } catch (Exception e) {
                if (sslMode == SslMode.PREFERRED) {
                    // still use non ssl channel
                    logger.info("{} still use non SSL channel due to SSL connect failed.", remoteSocketAddress, e);
                    return channel;
                }
                IOException ioe;
                if (e instanceof IOException) {
                    ioe = (IOException) e;
                } else {
                    ioe = new IOException(e);
                }
                throw ioe;
            }
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

package com.alibaba.otter.canal.parse.inbound.mongo.support;

import com.mongodb.ServerAddress;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * mongo集群连接信息
 * @author dsqin
 * @date 2018/5/15
 */
public class HaAuthentication {

    private List<ServerAddress> serverAddress;

    public List<ServerAddress> getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(List<ServerAddress> serverAddress) {
        this.serverAddress = serverAddress;
    }

    public HaAuthentication(List<ServerAddress> serverAddresses) {
        this.serverAddress = serverAddresses;
    }

    public InetSocketAddress getAddressIdentity() {
        if (null != serverAddress && !serverAddress.isEmpty()) {
            return  serverAddress.get(0).getSocketAddress();
        }
        return new InetSocketAddress("127.0.0.1", 9370);
    }
}

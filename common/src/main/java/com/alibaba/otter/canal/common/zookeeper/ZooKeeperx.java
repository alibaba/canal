package com.alibaba.otter.canal.common.zookeeper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.StaticHostProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

/**
 * 封装了ZooKeeper，使其支持节点的优先顺序，比如美国机房的节点会优先加载美国对应的zk集群列表，都失败后才会选择加载杭州的zk集群列表 *
 * 
 * @author jianghang 2012-7-10 下午02:31:42
 * @version 1.0.0
 */
public class ZooKeeperx extends ZkConnection {

    private static final String SERVER_COMMA            = ";";
    private static final Logger logger                  = LoggerFactory.getLogger(ZooKeeperx.class);
    private static final Field  clientCnxnField         = ReflectionUtils.findField(ZooKeeper.class, "cnxn");
    private static final Field  hostProviderField       = ReflectionUtils.findField(ClientCnxn.class, "hostProvider");
    private static final Field  serverAddressesField    = ReflectionUtils.findField(StaticHostProvider.class,
                                                            "serverAddresses");
    private static final Field  zookeeperLockField      = ReflectionUtils.findField(ZkConnection.class,
                                                            "_zookeeperLock");
    private static final Field  zookeeperFiled          = ReflectionUtils.findField(ZkConnection.class, "_zk");
    private static final int    DEFAULT_SESSION_TIMEOUT = 90000;

    private final List<String>  _serversList;
    private final int           _sessionTimeOut;

    public ZooKeeperx(String zkServers){
        this(zkServers, DEFAULT_SESSION_TIMEOUT);
    }

    public ZooKeeperx(String zkServers, int sessionTimeOut){
        super(zkServers, sessionTimeOut);
        _serversList = Arrays.asList(StringUtils.split(this.getServers(), SERVER_COMMA));
        _sessionTimeOut = sessionTimeOut;
    }

    @Override
    public void connect(Watcher watcher) {
        ReflectionUtils.makeAccessible(zookeeperLockField);
        ReflectionUtils.makeAccessible(zookeeperFiled);
        Lock _zookeeperLock = (ReentrantLock) ReflectionUtils.getField(zookeeperLockField, this);
        ZooKeeper _zk = (ZooKeeper) ReflectionUtils.getField(zookeeperFiled, this);

        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                throw new IllegalStateException("zk client has already been started");
            }
            String zkServers = _serversList.get(0);

            try {
                logger.debug("Creating new ZookKeeper instance to connect to " + zkServers + ".");
                _zk = new ZooKeeper(zkServers, _sessionTimeOut, watcher);
                configMutliCluster(_zk);
                ReflectionUtils.setField(zookeeperFiled, this, _zk);
            } catch (IOException e) {
                throw new ZkException("Unable to connect to " + zkServers, e);
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    // ===============================

    public void configMutliCluster(ZooKeeper zk) {
        if (_serversList.size() == 1) {
            return;
        }
        String cluster1 = _serversList.get(0);
        try {
            if (_serversList.size() > 1) {
                // 强制的声明accessible
                ReflectionUtils.makeAccessible(clientCnxnField);
                ReflectionUtils.makeAccessible(hostProviderField);
                ReflectionUtils.makeAccessible(serverAddressesField);

                // 添加第二组集群列表
                for (int i = 1; i < _serversList.size(); i++) {
                    String cluster = _serversList.get(i);
                    // 强制获取zk中的地址信息
                    ClientCnxn cnxn = (ClientCnxn) ReflectionUtils.getField(clientCnxnField, zk);
                    HostProvider hostProvider = (HostProvider) ReflectionUtils.getField(hostProviderField, cnxn);
                    List<InetSocketAddress> serverAddrs = (List<InetSocketAddress>) ReflectionUtils.getField(serverAddressesField,
                        hostProvider);
                    // 添加第二组集群列表
                    serverAddrs.addAll(new ConnectStringParser(cluster).getServerAddresses());
                }
            }
        } catch (Exception e) {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (InterruptedException ie) {
                // ignore interrupt
            }
            throw new ZkException("zookeeper_create_error, serveraddrs=" + cluster1, e);
        }

    }
}

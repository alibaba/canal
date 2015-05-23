package com.alibaba.otter.canal.common.zookeeper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.StaticHostProvider;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

/**
 * 封装了ZooKeeper，使其支持节点的优先顺序，比如美国机房的节点会优先加载美国对应的zk集群列表，都失败后才会选择加载杭州的zk集群列表 *
 * 
 * @author jianghang 2012-7-10 下午02:31:42
 * @version 1.0.0
 */
public class ZooKeeperx implements IZkConnection {

    private static final String SERVER_COMMA            = ";";
    private static final Logger logger                  = LoggerFactory.getLogger(ZooKeeperx.class);
    private static final Field  clientCnxnField         = ReflectionUtils.findField(ZooKeeper.class, "cnxn");
    private static final Field  hostProviderField       = ReflectionUtils.findField(ClientCnxn.class, "hostProvider");
    private static final Field  serverAddressesField    = ReflectionUtils.findField(StaticHostProvider.class,
                                                            "serverAddresses");
    private static final int    DEFAULT_SESSION_TIMEOUT = 90000;

    private ZooKeeper           _zk                     = null;
    private Lock                _zookeeperLock          = new ReentrantLock();

    private final List<String>  _servers;
    private final int           _sessionTimeOut;

    public ZooKeeperx(String zkServers){
        this(zkServers, DEFAULT_SESSION_TIMEOUT);
    }

    public ZooKeeperx(String zkServers, int sessionTimeOut){
        _servers = Arrays.asList(StringUtils.split(zkServers, SERVER_COMMA));
        _sessionTimeOut = sessionTimeOut;
    }

    @Override
    public void connect(Watcher watcher) {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                throw new IllegalStateException("zk client has already been started");
            }

            try {
                logger.debug("Creating new ZookKeeper instance to connect to " + _servers + ".");
                _zk = new ZooKeeper(_servers.get(0), _sessionTimeOut, watcher);
                configMutliCluster(_zk);
            } catch (IOException e) {
                throw new ZkException("Unable to connect to " + _servers, e);
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    public void close() throws InterruptedException {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                logger.debug("Closing ZooKeeper connected to " + _servers);
                _zk.close();
                _zk = null;
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    }

    public void delete(String path) throws InterruptedException, KeeperException {
        _zk.delete(path, -1);
    }

    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return _zk.exists(path, watch) != null;
    }

    public List<String> getChildren(final String path, final boolean watch) throws KeeperException,
                                                                           InterruptedException {
        return _zk.getChildren(path, watch);
    }

    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getData(path, watch, stat);
    }

    public void writeData(String path, byte[] data) throws KeeperException, InterruptedException {
        writeData(path, data, -1);
    }

    public void writeData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        _zk.setData(path, data, version);
    }

    public States getZookeeperState() {
        return _zk != null ? _zk.getState() : null;
    }

    public ZooKeeper getZookeeper() {
        return _zk;
    }

    public long getCreateTime(String path) throws KeeperException, InterruptedException {
        Stat stat = _zk.exists(path, false);
        if (stat != null) {
            return stat.getCtime();
        }
        return -1;
    }

    public String getServers() {
        return StringUtils.join(_servers, SERVER_COMMA);
    }

    // ===============================

    public void configMutliCluster(ZooKeeper zk) {
        if (_servers.size() == 1) {
            return;
        }
        String cluster1 = _servers.get(0);
        try {
            if (_servers.size() > 1) {
                // 强制的声明accessible
                ReflectionUtils.makeAccessible(clientCnxnField);
                ReflectionUtils.makeAccessible(hostProviderField);
                ReflectionUtils.makeAccessible(serverAddressesField);

                // 添加第二组集群列表
                for (int i = 1; i < _servers.size(); i++) {
                    String cluster = _servers.get(i);
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

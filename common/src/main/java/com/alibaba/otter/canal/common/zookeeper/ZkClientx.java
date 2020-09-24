package com.alibaba.otter.canal.common.zookeeper;

import java.util.Map;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;

import com.google.common.collect.MigrateMap;

/**
 * 使用自定义的ZooKeeperx for zk connection
 *
 * @author jianghang 2012-7-10 下午02:31:15
 * @version 1.0.0
 */
public class ZkClientx extends ZkClient {

    // 对于zkclient进行一次缓存，避免一个jvm内部使用多个zk connection
    private static Map<String, ZkClientx> clients = MigrateMap.makeComputingMap(ZkClientx::new);

    public static ZkClientx getZkClient(String servers) {
        return clients.get(servers);
    }

    public static void clearClients() {
        clients.clear();
    }

    public ZkClientx(String serverstring){
        this(serverstring, Integer.MAX_VALUE);
    }

    public ZkClientx(String zkServers, int connectionTimeout){
        this(new ZooKeeperx(zkServers), connectionTimeout);
    }

    public ZkClientx(String zkServers, int sessionTimeout, int connectionTimeout){
        this(new ZooKeeperx(zkServers, sessionTimeout), connectionTimeout);
    }

    public ZkClientx(String zkServers, int sessionTimeout, int connectionTimeout, ZkSerializer zkSerializer){
        this(new ZooKeeperx(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
    }

    private ZkClientx(IZkConnection connection, int connectionTimeout){
        this(connection, connectionTimeout, new ByteSerializer());
    }

    private ZkClientx(IZkConnection zkConnection, int connectionTimeout, ZkSerializer zkSerializer){
        super(zkConnection, connectionTimeout, zkSerializer);
    }

    /**
     * Create a persistent Sequential node.
     *
     * @param path
     * @param createParents if true all parent dirs are created as well and no
     * {@link ZkNodeExistsException} is thrown in case the path already exists
     * @throws ZkInterruptedException if operation was interrupted, or a
     * required reconnection got interrupted
     * @throws IllegalArgumentException if called from anything except the
     * ZooKeeper event thread
     * @throws ZkException if any ZooKeeper exception occurred
     * @throws RuntimeException if any other exception occurs
     */
    public String createPersistentSequential(String path, boolean createParents) throws ZkInterruptedException,
                                                                                IllegalArgumentException, ZkException,
                                                                                RuntimeException {
        try {
            return create(path, null, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(parentDir, createParents);
            return createPersistentSequential(path, createParents);
        }
    }

    /**
     * Create a persistent Sequential node.
     *
     * @param path
     * @param data
     * @param createParents if true all parent dirs are created as well and no
     * {@link ZkNodeExistsException} is thrown in case the path already exists
     * @throws ZkInterruptedException if operation was interrupted, or a
     * required reconnection got interrupted
     * @throws IllegalArgumentException if called from anything except the
     * ZooKeeper event thread
     * @throws ZkException if any ZooKeeper exception occurred
     * @throws RuntimeException if any other exception occurs
     */
    public String createPersistentSequential(String path, Object data, boolean createParents)
                                                                                             throws ZkInterruptedException,
                                                                                             IllegalArgumentException,
                                                                                             ZkException,
                                                                                             RuntimeException {
        try {
            return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(parentDir, createParents);
            return createPersistentSequential(path, data, createParents);
        }
    }

    /**
     * Create a persistent Sequential node.
     *
     * @param path
     * @param data
     * @param createParents if true all parent dirs are created as well and no
     * {@link ZkNodeExistsException} is thrown in case the path already exists
     * @throws ZkInterruptedException if operation was interrupted, or a
     * required reconnection got interrupted
     * @throws IllegalArgumentException if called from anything except the
     * ZooKeeper event thread
     * @throws ZkException if any ZooKeeper exception occurred
     * @throws RuntimeException if any other exception occurs
     */
    public void createPersistent(String path, Object data, boolean createParents) throws ZkInterruptedException,
                                                                                 IllegalArgumentException, ZkException,
                                                                                 RuntimeException {
        try {
            create(path, data, CreateMode.PERSISTENT);
        } catch (ZkNodeExistsException e) {
            if (!createParents) {
                throw e;
            }
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(parentDir, createParents);
            createPersistent(path, data, createParents);
        }
    }
}

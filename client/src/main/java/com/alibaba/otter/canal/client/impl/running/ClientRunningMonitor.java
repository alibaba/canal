package com.alibaba.otter.canal.client.impl.running;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;

/**
 * clinet running控制
 * 
 * @author jianghang 2012-11-22 下午03:43:01
 * @version 1.0.0
 */
public class ClientRunningMonitor extends AbstractCanalLifeCycle {

    private static final Logger        logger       = LoggerFactory.getLogger(ClientRunningMonitor.class);
    private ZkClientx                  zkClient;
    private String                     destination;
    private ClientRunningData          clientData;
    private IZkDataListener            dataListener;
    private BooleanMutex               mutex        = new BooleanMutex(false);
    private volatile boolean           release      = false;
    private volatile ClientRunningData activeData;
    private ScheduledExecutorService   delayExector = Executors.newScheduledThreadPool(1);
    private ClientRunningListener      listener;
    private int                        delayTime    = 5;

    public ClientRunningMonitor(){
        dataListener = new IZkDataListener() {

            public void handleDataChange(String dataPath, Object data) throws Exception {
                MDC.put("destination", destination);
                ClientRunningData runningData = JsonUtils.unmarshalFromByte((byte[]) data, ClientRunningData.class);
                if (!isMine(activeData.getAddress())) {
                    mutex.set(false);
                }

                if (!runningData.isActive() && isMine(activeData.getAddress())) { // 说明出现了主动释放的操作，并且本机之前是active
                    release = true;
                    releaseRunning();// 彻底释放mainstem
                }

                activeData = (ClientRunningData) runningData;
            }

            public void handleDataDeleted(String dataPath) throws Exception {
                MDC.put("destination", destination);
                mutex.set(false);
                if (!release && isMine(activeData.getAddress())) {
                    // 如果上一次active的状态就是本机，则即时触发一下active抢占
                    initRunning();
                } else {
                    // 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
                    delayExector.schedule(new Runnable() {

                        public void run() {
                            initRunning();
                        }
                    }, delayTime, TimeUnit.SECONDS);
                }
            }

        };

    }

    public void start() {
        super.start();

        String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, clientData.getClientId());
        zkClient.subscribeDataChanges(path, dataListener);
        initRunning();
    }

    public void stop() {
        super.stop();

        String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, clientData.getClientId());
        zkClient.unsubscribeDataChanges(path, dataListener);
        releaseRunning(); // 尝试一下release
    }

    public void initRunning() {
        if (!isStart()) {
            return;
        }

        String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, clientData.getClientId());
        // 序列化
        byte[] bytes = JsonUtils.marshalToByte(clientData);
        try {
            mutex.set(false);
            zkClient.create(path, bytes, CreateMode.EPHEMERAL);
            processActiveEnter();// 触发一下事件
            activeData = clientData;
            mutex.set(true);
        } catch (ZkNodeExistsException e) {
            bytes = zkClient.readData(path, true);
            if (bytes == null) {// 如果不存在节点，立即尝试一次
                initRunning();
            } else {
                activeData = JsonUtils.unmarshalFromByte(bytes, ClientRunningData.class);
            }
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(
                                      ZookeeperPathUtils.getClientIdNodePath(this.destination, clientData.getClientId()),
                                      true); // 尝试创建父节点
            initRunning();
        }
    }

    /**
     * 阻塞等待自己成为active，如果自己成为active，立马返回
     * 
     * @throws InterruptedException
     */
    public void waitForActive() throws InterruptedException {
        initRunning();
        mutex.get();
    }

    /**
     * 检查当前的状态
     */
    public boolean check() {
        String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, clientData.getClientId());
        try {
            byte[] bytes = zkClient.readData(path);
            ClientRunningData eventData = JsonUtils.unmarshalFromByte(bytes, ClientRunningData.class);
            activeData = eventData;// 更新下为最新值
            // 检查下nid是否为自己
            boolean result = isMine(activeData.getAddress());
            if (!result) {
                logger.warn("canal is running in [{}] , but not in [{}]", activeData.getAddress(),
                            clientData.getAddress());
            }
            return result;
        } catch (ZkNoNodeException e) {
            logger.warn("canal is not run any in node");
            return false;
        } catch (ZkInterruptedException e) {
            logger.warn("canal check is interrupt");
            Thread.interrupted();// 清除interrupt标记
            return check();
        } catch (ZkException e) {
            logger.warn("canal check is failed");
            return false;
        }
    }

    public boolean releaseRunning() {
        if (check()) {
            String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination, clientData.getClientId());
            zkClient.delete(path);
            mutex.set(false);
            processActiveExit();
            return true;
        }

        return false;
    }

    // ====================== helper method ======================

    private boolean isMine(String address) {
        return address.equals(clientData.getAddress());
    }

    private void processActiveEnter() {
        if (listener != null) {
            try {
                // 触发回调，建立与server的socket链接
                InetSocketAddress connectAddress = listener.processActiveEnter();
                String address = connectAddress.getAddress().getHostAddress() + ":" + connectAddress.getPort();
                this.clientData.setAddress(address);

                String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination,
                                                                             this.clientData.getClientId());
                // 序列化
                byte[] bytes = JsonUtils.marshalToByte(clientData);
                zkClient.writeData(path, bytes);
            } catch (Exception e) {
                logger.error("processSwitchActive failed", e);
            }
        }
    }

    private void processActiveExit() {
        if (listener != null) {
            try {
                listener.processActiveExit();
            } catch (Exception e) {
                logger.error("processSwitchActive failed", e);
            }
        }
    }

    public void setListener(ClientRunningListener listener) {
        this.listener = listener;
    }

    // ===================== setter / getter =======================

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setClientData(ClientRunningData clientData) {
        this.clientData = clientData;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public void setZkClient(ZkClientx zkClient) {
        this.zkClient = zkClient;
    }

}

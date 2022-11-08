package com.alibaba.otter.canal.client.impl.running;

import java.net.InetSocketAddress;
import java.text.MessageFormat;
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

import com.alibaba.otter.canal.client.impl.ServerNotFoundException;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;

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
                if (!isMine(runningData.getAddress())) {
                    mutex.set(false);
                }

                if (!runningData.isActive() && isMine(runningData.getAddress())) { // 说明出现了主动释放的操作，并且本机之前是active
                    release = true;
                    releaseRunning();// 彻底释放mainstem
                }

                activeData = (ClientRunningData) runningData;
            }

            public void handleDataDeleted(String dataPath) throws Exception {
                MDC.put("destination", destination);
                mutex.set(false);
                // 触发一下退出,可能是人为干预的释放操作或者网络闪断引起的session expired timeout
                processActiveExit();
                if (!release && activeData != null && isMine(activeData.getAddress())) {
                    // 如果上一次active的状态就是本机，则即时触发一下active抢占
                    initRunning();
                } else {
                    // 否则就是等待delayTime，避免因网络瞬端或者zk异常，导致出现频繁的切换操作
                    delayExector.schedule(() -> initRunning(), delayTime, TimeUnit.SECONDS);
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
        // Fix issue #697
        if (delayExector != null) {
            delayExector.shutdown();
        }
    }

    // 改动记录：
    // 1,在方法上加synchronized关键字,保证同步顺序执行;
    // 2,判断Zk上已经存在的activeData是否是本机，是的话把mutex重置为true，否则会导致死锁
    // 3,增加异常处理，保证出现异常时，running节点能被删除,否则会导致死锁
    public synchronized void initRunning() {
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
                // 如果发现已经存在,判断一下是否自己,避免活锁
                if (activeData.getAddress().contains(":") && isMine(activeData.getAddress())) {
                    mutex.set(true);
                }
            }
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(ZookeeperPathUtils.getClientIdNodePath(this.destination, clientData.getClientId()),
                true); // 尝试创建父节点
            initRunning();
        } catch (Throwable t) {
            logger.error(MessageFormat.format("There is an error when execute initRunning method, with destination [{0}].",
                destination),
                t);

            // fixed issue 1220, 针对server节点不工作避免死循环
            if (t instanceof ServerNotFoundException) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }

            // 出现任何异常尝试release
            releaseRunning();
            throw new CanalClientException("something goes wrong in initRunning method. ", t);
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
                logger.warn("canal is running in [{}] , but not in [{}]",
                    activeData.getAddress(),
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
            // 触发回调，建立与server的socket链接
            InetSocketAddress connectAddress = listener.processActiveEnter();
            String address = connectAddress.getAddress().getHostAddress() + ":" + connectAddress.getPort();
            this.clientData.setAddress(address);

            String path = ZookeeperPathUtils.getDestinationClientRunning(this.destination,
                this.clientData.getClientId());
            // 序列化
            byte[] bytes = JsonUtils.marshalToByte(clientData);
            zkClient.writeData(path, bytes);
        }
    }

    private void processActiveExit() {
        if (listener != null) {
            listener.processActiveExit();
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

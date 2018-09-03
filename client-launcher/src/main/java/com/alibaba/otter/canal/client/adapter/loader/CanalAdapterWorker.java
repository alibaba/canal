package com.alibaba.otter.canal.client.adapter.loader;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.adapter.CanalOuterAdapter;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 原生canal-server对应的client适配器工作线程
 *
 * @author machengyuan 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class CanalAdapterWorker extends AbstractCanalAdapterWorker {

    private static final int BATCH_SIZE = 50;
    private static final int SO_TIMEOUT = 0;

    private CanalConnector   connector;

    /**
     * 单台client适配器worker的构造方法
     *
     * @param canalDestination canal实例名
     * @param address canal-server地址
     * @param canalOuterAdapters 外部适配器组
     */
    public CanalAdapterWorker(String canalDestination, SocketAddress address,
                              List<List<CanalOuterAdapter>> canalOuterAdapters){
        this.canalOuterAdapters = canalOuterAdapters;
        this.canalDestination = canalDestination;
        groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        connector = CanalConnectors.newSingleConnector(address, canalDestination, "", "");
    }

    /**
     * HA模式下client适配器worker的构造方法
     *
     * @param canalDestination canal实例名
     * @param zookeeperHosts zookeeper地址
     * @param canalOuterAdapters 外部适配器组
     */
    public CanalAdapterWorker(String canalDestination, String zookeeperHosts,
                              List<List<CanalOuterAdapter>> canalOuterAdapters){
        this.canalOuterAdapters = canalOuterAdapters;
        this.canalDestination = canalDestination;
        groupInnerExecutorService = Executors.newFixedThreadPool(canalOuterAdapters.size());
        connector = CanalConnectors.newClusterConnector(zookeeperHosts, canalDestination, "", "");
        ((ClusterCanalConnector) connector).setSoTimeout(SO_TIMEOUT);

        // super.initSwitcher(canalDestination);
    }

    @Override
    public void start() {
        if (!running) {
            thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    process();
                }
            });
            thread.setUncaughtExceptionHandler(handler);
            thread.start();
            running = true;
        }
    }

    @Override
    public void stop() {
        try {
            if (!running) {
                return;
            }

            // if (switcher != null && !switcher.state()) {
            // switcher.set(true);
            // }

            connector.stopRunning();
            running = false;

            logger.info("destination {} is waiting for adapters' worker thread die!", canalDestination);
            if (thread != null) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            groupInnerExecutorService.shutdown();
            logger.info("destination {} adapters' worker thread dead!", canalDestination);
            for (List<CanalOuterAdapter> outerAdapters : canalOuterAdapters) {
                for (CanalOuterAdapter adapter : outerAdapters) {
                    adapter.destroy();
                }
            }
            logger.info("destination {} all adapters destroyed!", canalDestination);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void process() {
        while (!running)
            ; // waiting until running == true
        while (running) {
            try {
                // if (switcher != null) {
                // switcher.get();
                // }
                logger.info("=============> Start to connect destination: {} <=============", this.canalDestination);
                connector.connect();
                logger.info("=============> Start to subscribe destination: {} <=============", this.canalDestination);
                connector.subscribe();
                logger.info("=============> Subscribe destination: {} succeed <=============", this.canalDestination);
                while (running) {
                    // try {
                    // if (switcher != null) {
                    // switcher.get();
                    // }
                    // } catch (TimeoutException e) {
                    // break;
                    // }

                    // server配置canal.instance.network.soTimeout(默认: 30s)
                    // 范围内未与server交互，server将关闭本次socket连接
                    Message message = connector.getWithoutAck(BATCH_SIZE); // 获取指定数量的数据
                    long batchId = message.getId();
                    try {
                        int size = message.getEntries().size();

                        if (batchId == -1 || size == 0) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug("destination: {} batchId: {} batchSize: {} ",
                                    this.canalDestination,
                                    batchId,
                                    size);
                            }
                            long begin = System.currentTimeMillis();
                            writeOut(message);
                            long now = System.currentTimeMillis();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destination: {} batchId: {} elapsed time: {} ms",
                                    this.canalDestination,
                                    batchId,
                                    now - begin);
                            }
                        }
                        connector.ack(batchId); // 提交确认
                    } catch (Exception e) {
                        connector.rollback(batchId); // 处理失败, 回滚数据
                        throw e;
                    }
                }

            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                logger.info("=============> Disconnect destination: {} <=============", this.canalDestination);
            }

            if (running) { // is reconnect
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

        }
    }
}

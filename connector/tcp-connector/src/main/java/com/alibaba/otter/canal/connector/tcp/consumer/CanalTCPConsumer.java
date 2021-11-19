package com.alibaba.otter.canal.connector.tcp.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.client.impl.ClusterNodeAccessStrategy;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.connector.core.consumer.CommonMessage;
import com.alibaba.otter.canal.connector.core.spi.CanalMsgConsumer;
import com.alibaba.otter.canal.connector.core.spi.SPI;
import com.alibaba.otter.canal.connector.core.util.MessageUtil;
import com.alibaba.otter.canal.connector.tcp.config.TCPConstants;
import com.alibaba.otter.canal.protocol.Message;

import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * TCP 消费者连接器, 一个destination对应一个SPI实例
 *
 * @author rewerma 2020-01-30
 * @author XuDaojie
 * @version 1.1.5
 * @since 1.1.5
 */
@SPI("tcp")
public class CanalTCPConsumer implements CanalMsgConsumer {

    private Long           currentBatchId = null;
    private CanalConnector canalConnector;
    private int            batchSize      = 500;

    @Override
    public void init(Properties properties, String destination, String groupId) {
        // load config
        String host = properties.getProperty(TCPConstants.CANAL_TCP_HOST);
        String username = properties.getProperty(TCPConstants.CANAL_TCP_USERNAME);
        String password = properties.getProperty(TCPConstants.CANAL_TCP_PASSWORD);
        String zkHosts = properties.getProperty(TCPConstants.CANAL_TCP_ZK_HOSTS);
        String batchSizePro = properties.getProperty(TCPConstants.CANAL_TCP_BATCH_SIZE);
        if (batchSizePro != null) {
            batchSize = Integer.parseInt(batchSizePro);
        }
        if (StringUtils.isNotBlank(host)) {
            String[] ipPort = host.split(":");
            SocketAddress sa = new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1]));
            this.canalConnector = new SimpleCanalConnector(sa, username, password, destination);
        } else {
            this.canalConnector = new ClusterCanalConnector(username,
                password,
                destination,
                new ClusterNodeAccessStrategy(destination, ZkClientx.getZkClient(zkHosts)));
        }
    }

    @Override
    public void connect() {
        canalConnector.connect();
        canalConnector.subscribe();
    }

    @Override
    public List<CommonMessage> getMessage(Long timeout, TimeUnit unit) {
        try {
            Message message = canalConnector.getWithoutAck(batchSize, timeout, unit);
            long batchId = message.getId();
            currentBatchId = batchId;
            int size = message.getEntries().size();
            if (batchId == -1 || size == 0) {
                return null;
            } else {
                return MessageUtil.convert(message);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollback() {
        if (currentBatchId != null && currentBatchId != -1) {
            canalConnector.rollback(currentBatchId);
            currentBatchId = null;
        }
    }

    @Override
    public void ack() {
        if (currentBatchId != null) {
            canalConnector.ack(currentBatchId);
            currentBatchId = null;
        }
    }

    @Override
    public void disconnect() {
        canalConnector.unsubscribe();
        canalConnector.disconnect();
    }
}

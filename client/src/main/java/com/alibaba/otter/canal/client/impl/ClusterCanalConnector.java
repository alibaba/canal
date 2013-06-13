package com.alibaba.otter.canal.client.impl;

import java.net.SocketAddress;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalNodeAccessStrategy;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;

/**
 * 集群版本connector实现，自带了failover功能<br/>
 * 
 * @author jianghang 2012-10-29 下午08:04:06
 * @version 1.0.0
 */
public class ClusterCanalConnector implements CanalConnector {

    private final Logger            logger        = LoggerFactory.getLogger(this.getClass());
    private String                  username;
    private String                  password;
    private int                     soTimeout     = 10000;
    private int                     retryTimes    = 3;
    private int                     retryInterval = 5000;                                    // 重试的时间间隔，默认5秒
    private CanalNodeAccessStrategy accessStrategy;
    private SimpleCanalConnector    currentConnector;
    private String                  destination;

    public ClusterCanalConnector(String username, String password, String destination,
                                 CanalNodeAccessStrategy accessStrategy){
        this.username = username;
        this.password = password;
        this.destination = destination;
        this.accessStrategy = accessStrategy;
    }

    public void connect() throws CanalClientException {
        while (currentConnector == null) {
            SocketAddress nextAddress = this.accessStrategy.nextNode();

            int times = 0;
            /**
             * retry for #retryTimes for each node when trying to connect to it.
             */
            while (times < retryTimes) {
                try {
                    currentConnector = new SimpleCanalConnector(nextAddress, username, password, destination);
                    currentConnector.setSoTimeout(soTimeout);
                    if (accessStrategy instanceof ClusterNodeAccessStrategy) {
                        currentConnector.setZkClientx(((ClusterNodeAccessStrategy) accessStrategy).getZkClient());
                    }

                    currentConnector.connect();
                    break;
                } catch (Exception e) {
                    logger.warn("failed to connect to:{} after retry {} times", nextAddress, times);
                    currentConnector.disconnect();
                    currentConnector = null;
                    times = times + 1;
                }
            }
        }
    }

    public boolean checkValid() {
        return currentConnector != null && currentConnector.checkValid();
    }

    public void disconnect() throws CanalClientException {
        if (currentConnector != null) {
            currentConnector.disconnect();
            currentConnector = null;
        }
    }

    public void subscribe(String filter) throws CanalClientException {
        int times = 0;
        while (times < retryTimes) {
            try {
                currentConnector.subscribe(filter);
                return;
            } catch (Throwable t) {
                logger.warn("something goes wrong when subscribing from server:{}\n{}", currentConnector.getAddress(),
                            ExceptionUtils.getFullStackTrace(t));
                times++;
                restart();
                logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to subscribe after " + times + " times retry.");
    }

    public void unsubscribe() throws CanalClientException {
        int times = 0;
        while (times < retryTimes) {
            try {
                currentConnector.unsubscribe();
                return;
            } catch (Throwable t) {
                logger.warn("something goes wrong when unsubscribing from server:{}\n{}",
                            currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(t));
                times++;
                restart();
                logger.info("restart the connector for next round retry.");
            }
        }
        throw new CanalClientException("failed to unsubscribe after " + times + " times retry.");
    }

    public Message get(int batchSize) throws CanalClientException {
        int times = 0;
        while (times < retryTimes) {
            try {
                Message msg = currentConnector.get(batchSize);
                return msg;
            } catch (Throwable t) {
                logger.warn("something goes wrong when getting data from server:{}\n{}", currentConnector.getAddress(),
                            ExceptionUtils.getFullStackTrace(t));
                times++;
                restart();
                logger.info("restart the connector for next round retry.");
            }
        }
        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public Message getWithoutAck(int batchSize) throws CanalClientException {
        int times = 0;
        while (times < retryTimes) {
            try {
                Message msg = currentConnector.getWithoutAck(batchSize);
                return msg;
            } catch (Throwable t) {
                logger.warn("something goes wrong when getWithoutAck data from server:{}\n{}",
                            currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(t));
                times++;
                restart();
                logger.info("restart the connector for next round retry.");
            }
        }
        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public void rollback(long batchId) throws CanalClientException {
        int times = 0;
        while (times < retryTimes) {
            try {
                currentConnector.rollback(batchId);
                return;
            } catch (Throwable t) {
                logger.warn("something goes wrong when rollbacking data from server:{}\n{}",
                            currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(t));
                times++;
                restart();
                logger.info("restart the connector for next round retry.");
            }
        }
        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public void rollback() throws CanalClientException {
        int times = 0;
        while (times < retryTimes) {
            try {
                currentConnector.rollback();
                return;
            } catch (Throwable t) {
                logger.warn("something goes wrong when rollbacking data from server:{}\n{}",
                            currentConnector.getAddress(), ExceptionUtils.getFullStackTrace(t));
                times++;
                restart();
                logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    public void ack(long batchId) throws CanalClientException {
        int times = 0;
        while (times < retryTimes) {
            try {
                currentConnector.ack(batchId);
                return;
            } catch (Throwable t) {
                logger.warn("something goes wrong when acking data from server:{}\n{}", currentConnector.getAddress(),
                            ExceptionUtils.getFullStackTrace(t));
                times++;
                restart();
                logger.info("restart the connector for next round retry.");
            }
        }

        throw new CanalClientException("failed to fetch the data after " + times + " times retry");
    }

    private void restart() throws CanalClientException {
        disconnect();
        try {
            Thread.sleep(retryInterval);
        } catch (InterruptedException e) {
            throw new CanalClientException(e);
        }
        connect();
    }

    // ============================= setter / getter ============================

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    
    public int getRetryInterval() {
        return retryInterval;
    }

    
    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    
    public CanalNodeAccessStrategy getAccessStrategy() {
        return accessStrategy;
    }

    
    public void setAccessStrategy(CanalNodeAccessStrategy accessStrategy) {
        this.accessStrategy = accessStrategy;
    }

    
    public SimpleCanalConnector getCurrentConnector() {
        return currentConnector;
    }

}

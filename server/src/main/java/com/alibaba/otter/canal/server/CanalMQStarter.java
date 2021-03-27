package com.alibaba.otter.canal.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalMQConfig;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

public class CanalMQStarter {

    private static final Logger          logger         = LoggerFactory.getLogger(CanalMQStarter.class);

    private volatile boolean             running        = false;

    private ExecutorService              executorService;

    private CanalMQProducer              canalMQProducer;

    private MQProperties                 mqProperties;

    private CanalServerWithEmbedded      canalServer;

    private Map<String, CanalMQRunnable> canalMQWorks   = new ConcurrentHashMap<>();

    private static Thread                shutdownThread = null;

    public CanalMQStarter(CanalMQProducer canalMQProducer){
        this.canalMQProducer = canalMQProducer;
    }

    public synchronized void start(String destinations) {
        try {
            if (running) {
                return;
            }
            mqProperties = canalMQProducer.getMqProperties();
            // set filterTransactionEntry
            if (mqProperties.isFilterTransactionEntry()) {
                System.setProperty("canal.instance.filter.transaction.entry", "true");
            }

            canalServer = CanalServerWithEmbedded.instance();

            // 对应每个instance启动一个worker线程
            executorService = Executors.newCachedThreadPool();
            logger.info("## start the MQ workers.");

            String[] dsts = StringUtils.split(destinations, ",");
            for (String destination : dsts) {
                destination = destination.trim();
                CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
                canalMQWorks.put(destination, canalMQRunnable);
                executorService.execute(canalMQRunnable);
            }

            running = true;
            logger.info("## the MQ workers is running now ......");

            shutdownThread = new Thread(() -> {
                try {
                    logger.info("## stop the MQ workers");
                    running = false;
                    executorService.shutdown();
                    canalMQProducer.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping MQ workers:", e);
                } finally {
                    logger.info("## canal MQ is down.");
                }
            });

            Runtime.getRuntime().addShutdownHook(shutdownThread);
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal MQ workers:", e);
        }
    }

    public synchronized void destroy() {
        running = false;
        if (executorService != null) {
            executorService.shutdown();
        }
        if (canalMQProducer != null) {
            canalMQProducer.stop();
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
    }

    public synchronized void startDestination(String destination) {
        CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
        if (canalInstance != null) {
            stopDestination(destination);
            CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
            canalMQWorks.put(canalInstance.getDestination(), canalMQRunnable);
            executorService.execute(canalMQRunnable);
            logger.info("## Start the MQ work of destination:" + destination);
        }
    }

    public synchronized void stopDestination(String destination) {
        CanalMQRunnable canalMQRunable = canalMQWorks.get(destination);
        if (canalMQRunable != null) {
            canalMQRunable.stop();
            canalMQWorks.remove(destination);
            logger.info("## Stop the MQ work of destination:" + destination);
        }
    }

    private void worker(String destination, AtomicBoolean destinationRunning) {
        while (!running || !destinationRunning.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        logger.info("## start the MQ producer: {}.", destination);
        MDC.put("destination", destination);
        final ClientIdentity clientIdentity = new ClientIdentity(destination, (short) 1001, "");
        while (running && destinationRunning.get()) {
            try {
                CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
                if (canalInstance == null) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    continue;
                }
                MQDestination canalDestination = new MQDestination();
                canalDestination.setCanalDestination(destination);
                CanalMQConfig mqConfig = canalInstance.getMqConfig();
                canalDestination.setTopic(mqConfig.getTopic());
                canalDestination.setPartition(mqConfig.getPartition());
                canalDestination.setDynamicTopic(mqConfig.getDynamicTopic());
                canalDestination.setPartitionsNum(mqConfig.getPartitionsNum());
                canalDestination.setPartitionHash(mqConfig.getPartitionHash());
                canalDestination.setDynamicTopicPartitionNum(mqConfig.getDynamicTopicPartitionNum());

                canalServer.subscribe(clientIdentity);
                logger.info("## the MQ producer: {} is running now ......", destination);

                Integer getTimeout = mqProperties.getFetchTimeout();
                Integer getBatchSize = mqProperties.getBatchSize();
                while (running && destinationRunning.get()) {
                    Message message;
                    if (getTimeout != null && getTimeout > 0) {
                        message = canalServer.getWithoutAck(clientIdentity,
                            getBatchSize,
                            getTimeout.longValue(),
                            TimeUnit.MILLISECONDS);
                    } else {
                        message = canalServer.getWithoutAck(clientIdentity, getBatchSize);
                    }

                    final long batchId = message.getId();
                    try {
                        int size = message.isRaw() ? message.getRawEntries().size() : message.getEntries().size();
                        if (batchId != -1 && size != 0) {
                            canalMQProducer.send(canalDestination, message, new Callback() {

                                @Override
                                public void commit() {
                                    canalServer.ack(clientIdentity, batchId); // 提交确认
                                }

                                @Override
                                public void rollback() {
                                    canalServer.rollback(clientIdentity, batchId);
                                }
                            }); // 发送message到topic
                        } else {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }

                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            }
        }
    }

    private class CanalMQRunnable implements Runnable {

        private String destination;

        CanalMQRunnable(String destination){
            this.destination = destination;
        }

        private AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void run() {
            worker(destination, running);
        }

        public void stop() {
            running.set(false);
        }
    }
}

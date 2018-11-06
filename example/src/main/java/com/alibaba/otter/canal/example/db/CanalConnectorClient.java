package com.alibaba.otter.canal.example.db;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public abstract class CanalConnectorClient extends AbstractCanalLifeCycle implements InitializingBean {

    protected static final Logger logger = LoggerFactory.getLogger(CanalConnectorClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static String contextFormat;
    protected static String rowFormat;
    protected static String transactionFormat;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    static {
        StringBuilder sb = new StringBuilder();
        sb.append(SEP)
                .append("-------------Batch-------------")
                .append(SEP)
                .append("* Batch Id: [{}] ,count : [{}] , Mem size : [{}] , Time : {}")
                .append(SEP)
                .append("* Start : [{}] ")
                .append(SEP)
                .append("* End : [{}] ")
                .append(SEP)
                .append("-------------------------------")
                .append(SEP);
        contextFormat = sb.toString();

        sb = new StringBuilder();
        sb.append(SEP)
                .append("+++++++++++++Row+++++++++++++>>>")
                .append("binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms")
                .append(SEP);
        rowFormat = sb.toString();

        sb = new StringBuilder();
        sb.append(SEP)
                .append("===========Transaction {} : {}=======>>>")
                .append("binlog[{}:{}] , executeTime : {} , delay : {}ms")
                .append(SEP);
        transactionFormat = sb.toString();
    }

    private String zkServers;//cluster
    private String address;//single，ip:port
    private String destination;
    private String username;
    private String password;
    private int batchSize = 5 * 1024;
    private String filter = "";//同canal filter，用于过滤database或者table的相关数据。
    protected boolean debug = false;//开启debug，会把每条消息的详情打印

    //1:retry，重试，重试默认为3次，由retryTimes参数决定，如果重试次数达到阈值，则跳过，并且记录日志。
    //2:ignore,直接忽略，不重试，记录日志。
    protected int exceptionStrategy = 1;
    protected int retryTimes = 3;
    protected int waitingTime = 100;//当binlog没有数据时，主线程等待的时间，单位ms,大于0


    protected CanalConnector connector;
    protected Thread thread;

    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        public void uncaughtException(Thread t, Throwable e) {
            logger.error("process message has an error", e);
        }
    };

    @Override
    public void afterPropertiesSet() {
        if (waitingTime <= 0) {
            throw new IllegalArgumentException("waitingTime must be greater than 0");
        }
        if (ExceptionStrategy.codeOf(exceptionStrategy) == null) {
            throw new IllegalArgumentException("exceptionStrategy is not valid,1 or 2");
        }
        start();
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        super.start();
        initConnector();

        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        super.stop();
        quietlyStop(thread);
    }

    protected void quietlyStop(Thread task) {
        if (task != null) {
            task.interrupt();
            try {
                task.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public void process() {
        int times = 0;
        while (running) {
            try {
                sleepWhenFailed(times);
                //after block, should check the status of thread.
                if (!running) {
                    break;
                }
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe(filter);
                connector.rollback();
                times = 0;//reset;

                while (running) {
                    // 获取指定数量的数据，不确认
                    Message message = connector.getWithoutAck(batchSize);

                    long batchId = message.getId();
                    int size = message.getEntries().size();

                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(waitingTime);
                        } catch (InterruptedException e) {
                            //
                        }
                        continue;
                    }
                    //logger
                    printBatch(message, batchId);

                    processMessage(message);

                }
            } catch (Exception e) {
                logger.error("process error!", e);
                if (times > 20) {
                    times = 0;
                }
                times++;
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    protected abstract void processMessage(Message message);


    private void initConnector() {
        if (zkServers != null && zkServers.length() > 0) {
            connector = CanalConnectors.newClusterConnector(zkServers, destination, username, password);
        } else if (address != null) {
            String[] segments = address.split(":");
            SocketAddress socketAddress = new InetSocketAddress(segments[0], Integer.valueOf(segments[1]));
            connector = CanalConnectors.newSingleConnector(socketAddress, destination, username, password);
        } else {
            throw new IllegalArgumentException("zkServers or address cant be null at same time,you should specify one of them!");
        }

    }

    /**
     * 用于控制当连接异常时，重试的策略，我们不应该每次都是立即重试，否则将可能导致大量的错误，在空转时导致CPU过高的问题
     * sleep策略基于简单的累加
     *
     * @param times
     */
    private void sleepWhenFailed(int times) {
        if (times <= 0) {
            return;
        }
        try {
            int sleepTime = 1000 + times * 100;//最大sleep 3s。
            Thread.sleep(sleepTime);
        } catch (Exception ex) {
            //
        }
    }

    /**
     * 打印当前batch的摘要信息
     *
     * @param message
     * @param batchId
     */
    protected void printBatch(Message message, long batchId) {
        if (!debug) {
            return;
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        if (CollectionUtils.isEmpty(entries)) {
            return;
        }

        long memSize = 0;
        for (CanalEntry.Entry entry : entries) {
            memSize += entry.getHeader().getEventLength();
        }
        int size = entries.size();
        String startPosition = buildPosition(entries.get(0));
        String endPosition = buildPosition(message.getEntries().get(size - 1));

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(contextFormat, new Object[]{batchId, size, memSize, format.format(new Date()), startPosition, endPosition});
    }

    protected String buildPosition(CanalEntry.Entry entry) {
        CanalEntry.Header header = entry.getHeader();
        long time = header.getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        StringBuilder sb = new StringBuilder();
        sb.append(header.getLogfileName())
                .append(":")
                .append(header.getLogfileOffset())
                .append(":")
                .append(header.getExecuteTime())
                .append("(")
                .append(format.format(date))
                .append(")");
        return sb.toString();
    }

    /**
     * default，only logging information
     *
     * @param entry
     */
    protected void transactionBegin(CanalEntry.Entry entry) {
        if (!debug) {
            return;
        }
        try {
            CanalEntry.TransactionBegin begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
            // 打印事务头信息，执行的线程id，事务耗时
            CanalEntry.Header header = entry.getHeader();
            long executeTime = header.getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            logger.info(transactionFormat,
                    new Object[]{"begin", begin.getTransactionId(), header.getLogfileName(),
                            String.valueOf(header.getLogfileOffset()),
                            String.valueOf(header.getExecuteTime()), String.valueOf(delayTime)});
        } catch (Exception e) {
            logger.error("parse event has an error , data:" + entry.toString(), e);
        }
    }

    protected void transactionEnd(CanalEntry.Entry entry) {
        if (!debug) {
            return;
        }
        try {
            CanalEntry.TransactionEnd end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
            // 打印事务提交信息，事务id
            CanalEntry.Header header = entry.getHeader();
            long executeTime = header.getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            logger.info(transactionFormat,
                    new Object[]{"end", end.getTransactionId(), header.getLogfileName(),
                            String.valueOf(header.getLogfileOffset()),
                            String.valueOf(header.getExecuteTime()), String.valueOf(delayTime)});
        } catch (Exception e) {
            logger.error("parse event has an error , data:" + entry.toString(), e);
        }
    }

    /**
     * 判断事件类型为DML 数据
     *
     * @param eventType
     * @return
     */
    protected boolean isDML(CanalEntry.EventType eventType) {
        switch (eventType) {
            case INSERT:
            case UPDATE:
            case DELETE:
                return true;
            default:
                return false;
        }
    }

    /**
     * 处理 DDL数据
     *
     * @param header
     * @param eventType
     * @param sql
     */

    protected void processDDL(CanalEntry.Header header, CanalEntry.EventType eventType, String sql) {
        if (!debug) {
            return;
        }
        String table = header.getSchemaName() + "." + header.getTableName();
        //对于DDL，直接执行，因为没有行变更数据
        switch (eventType) {
            case CREATE:
                logger.warn("parse create table event, table: {}, sql: {}", table, sql);
                return;
            case ALTER:
                logger.warn("parse alter table event, table: {}, sql: {}", table, sql);
                return;
            case TRUNCATE:
                logger.warn("parse truncate table event, table: {}, sql: {}", table, sql);
                return;
            case ERASE:
            case QUERY:
                logger.warn("parse event : {}, sql: {} . ignored!", eventType.name(), sql);
                return;
            case RENAME:
                logger.warn("parse rename table event, table: {}, sql: {}", table, sql);
                return;
            case CINDEX:
                logger.warn("parse create index event, table: {}, sql: {}", table, sql);
                return;
            case DINDEX:
                logger.warn("parse delete index event, table: {}, sql: {}", table, sql);
                return;
            default:
                logger.warn("parse unknown event: {}, table: {}, sql: {}", eventType.name(), table, sql);
                break;
        }
    }

    /**
     * 强烈建议捕获异常，非上述已列出的其他操作，非核心
     * 除了“insert”、“update”、“delete”操作之外的，其他类型的操作.
     * 默认实现为“无操作”
     *
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    public void whenOthers(CanalEntry.Header header, String sql) {
        String schema = header.getSchemaName();
        String table = header.getTableName();
        logger.error("ignore event,schema: {},table: {},SQL: {}", schema, table, sql);
    }

    public enum ExceptionStrategy {
        RETRY(1), IGNORE(2);
        public int code;

        ExceptionStrategy(int code) {
            this.code = code;
        }

        public static ExceptionStrategy codeOf(Integer code) {
            if (code != null) {
                for (ExceptionStrategy e : ExceptionStrategy.values()) {
                    if (e.code == code) {
                        return e;
                    }
                }
            }
            return null;
        }
    }

    public String getZkServers() {
        return zkServers;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

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

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public int getExceptionStrategy() {
        return exceptionStrategy;
    }

    public void setExceptionStrategy(int exceptionStrategy) {
        this.exceptionStrategy = exceptionStrategy;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getWaitingTime() {
        return waitingTime;
    }

    public void setWaitingTime(int waitingTime) {
        this.waitingTime = waitingTime;
    }
}

package com.alibaba.otter.canal.client.adapter.rdb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchExecutor {

    private static final Logger logger     = LoggerFactory.getLogger(BatchExecutor.class);

    private Integer             key;
    private Connection          conn;
    private int                 commitSize = 3000;
    private AtomicInteger       idx        = new AtomicInteger(0);
    private ExecutorService     executor   = Executors.newFixedThreadPool(1);
    private Lock                commitLock = new ReentrantLock();
    private Condition           condition  = commitLock.newCondition();

    public BatchExecutor(Integer key, Connection conn, Integer commitSize){
        this.key = key;
        this.conn = conn;
        if (commitSize != null) {
            this.commitSize = commitSize;
        }

        try {
            this.conn.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public Integer getKey() {
        return key;
    }

    public Connection getConn() {
        return conn;
    }

    public static void setValue(List<Map<String, ?>> values, int type, Object value) {
        Map<String, Object> valueItem = new HashMap<>();
        valueItem.put("type", type);
        valueItem.put("value", value);
        values.add(valueItem);
    }

    public void execute(String sql, List<Map<String, ?>> values) {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int len = values.size();
            for (int i = 0; i < len; i++) {
                int type = (Integer) values.get(i).get("type");
                Object value = values.get(i).get("value");
                RdbSyncService.setPStmt(type, pstmt, value, i + 1);
            }

            pstmt.execute();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        int i = idx.incrementAndGet();

        // 批次的第一次执行设置延时
        if (i == 1) {
            executor.submit(() -> {
                try {
                    commitLock.lock();
                    if (!condition.await(5, TimeUnit.SECONDS)) {
                        // 超时提交
                        commit();
                    }
                } catch (InterruptedException e) {
                    // ignore
                } finally {
                    commitLock.unlock();
                }
            });
        }

        if (i == commitSize) {
            commit();
        }
    }

    private void commit() {
        try {
            commitLock.lock();
            conn.commit();
            if (logger.isTraceEnabled()) {
                logger.trace("Batch executor: " + key + " commit " + idx.get() + " rows");
            }
            condition.signal();
            idx.set(0);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            commitLock.unlock();
        }
    }

    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
        executor.shutdown();
    }
}

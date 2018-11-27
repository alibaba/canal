package com.alibaba.otter.canal.client.adapter.rdb.support;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchExecutor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(BatchExecutor.class);

    private Integer             key;
    private Connection          conn;
    private AtomicInteger       idx    = new AtomicInteger(0);

    public BatchExecutor(Connection conn){
        this(1, conn);
    }

    public BatchExecutor(Integer key, Connection conn){
        this.key = key;
        this.conn = conn;

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
                SyncUtil.setPStmt(type, pstmt, value, i + 1);
            }

            pstmt.execute();
            idx.incrementAndGet();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void commit() {
        try {
            conn.commit();
            if (logger.isTraceEnabled()) {
                logger.trace("Batch executor: " + key + " commit " + idx.get() + " rows");
            }
            idx.set(0);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}

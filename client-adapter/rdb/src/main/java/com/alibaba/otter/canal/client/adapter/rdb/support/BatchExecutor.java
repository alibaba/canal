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

    private Connection          conn;
    private AtomicInteger       idx    = new AtomicInteger(0);

    public BatchExecutor(Connection conn) throws SQLException{
        this.conn = conn;
        this.conn.setAutoCommit(false);
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

    public void execute(String sql, List<Map<String, ?>> values) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        int len = values.size();
        for (int i = 0; i < len; i++) {
            int type = (Integer) values.get(i).get("type");
            Object value = values.get(i).get("value");
            SyncUtil.setPStmt(type, pstmt, value, i + 1);
        }

        pstmt.execute();
        idx.incrementAndGet();
    }

    public void commit() throws SQLException {
        conn.commit();
        if (logger.isTraceEnabled()) {
            logger.trace("Batch executor commit " + idx.get() + " rows");
        }
        idx.set(0);
    }

    public void rollback() throws SQLException {
        conn.rollback();
        if (logger.isTraceEnabled()) {
            logger.trace("Batch executor rollback " + idx.get() + " rows");
        }
        idx.set(0);
    }

    @Override
    public void close() {
        if (conn != null) {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ioe) {
            }
        }
    }
}

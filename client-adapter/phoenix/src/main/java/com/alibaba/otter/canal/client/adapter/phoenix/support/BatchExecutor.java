package com.alibaba.otter.canal.client.adapter.phoenix.support;

import com.alibaba.otter.canal.client.adapter.phoenix.PhoenixAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * sql批量执行器   基本没有变动
 */
public class BatchExecutor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(BatchExecutor.class);

    private DataSource          dataSource;
    private Connection          conn;
    private AtomicInteger       idx    = new AtomicInteger(0);

    public BatchExecutor(DataSource dataSource){
        this.dataSource = dataSource;
    }
    public BatchExecutor(){
    }

    public Connection getConn() {
        if (conn == null) {
            try {
                conn=PhoenixAdapter.getPhoenixConnection();
                this.conn.setAutoCommit(false);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return conn;
    }

    public static void setValue(List<Map<String, ?>> values, int type, Object value) {
        Map<String, Object> valueItem = new HashMap<>();
        valueItem.put("type", type);
        valueItem.put("value", value);
        values.add(valueItem);
    }

    public int executeUpdate(String sql) throws SQLException {
        logger.debug("execute: {}", sql);
        Statement statement = getConn().createStatement();
        int ret = statement.executeUpdate(sql);
        statement.close();
        return ret;
    }

    public void execute(String sql, List<Map<String, ?>> values) throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug("execute: {} {}", sql, Arrays.toString(values.toArray()));
        }
        PreparedStatement pstmt = getConn().prepareStatement(sql);
        int len = values.size();
        for (int i = 0; i < len; i++) {
            int type = (Integer) values.get(i).get("type");
            Object value = values.get(i).get("value");
            SyncUtil.setPStmt(type, pstmt, value, i + 1);
        }

        pstmt.execute();
        idx.incrementAndGet();
        pstmt.close();
    }

    public void commit() throws SQLException {
        getConn().commit();
        if (logger.isTraceEnabled()) {
            logger.trace("Batch executor commit " + idx.get() + " rows");
        }
        idx.set(0);
    }

    public void rollback() throws SQLException {
        getConn().rollback();
        if (logger.isTraceEnabled()) {
            logger.trace("Batch executor rollback " + idx.get() + " rows");
        }
        idx.set(0);
    }

    @Override
    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            } finally {
                conn = null;
            }
        }
    }
}

package com.alibaba.otter.canal.client.adapter.support;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    /**
     * 通过DS执行sql
     */
    public static Object sqlRS(DataSource ds, String sql, Function<ResultSet, Object> fun) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(sql);
            return fun.apply(rs);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * sql执行获取resultSet
     *
     * @param conn sql connection
     * @param sql sql
     * @param consumer 回调方法
     */
    public static void sqlRS(Connection conn, String sql, Consumer<ResultSet> consumer) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            consumer.accept(rs);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}

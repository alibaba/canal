package com.alibaba.otter.canal.client.adapter.phoenix.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Function;

/**
 * @author: lihua
 * @date: 2020/12/30 18:43
 * @Description:
 */
public class PhoenixSupportUtil {


    public static final  Logger logger = LoggerFactory.getLogger(PhoenixSupportUtil.class);
    public static Object sqlRS(Connection dsConnection, String sql, Function<ResultSet, Object> fun) {

        try {
            Connection conn = dsConnection;
            Throwable var4 = null;

            Object var9;
            try {
                Statement stmt = conn.createStatement(1003, 1007);
                Throwable var6 = null;

                try {
                    stmt.setFetchSize(-2147483648);
                    ResultSet rs = stmt.executeQuery(sql);
                    Throwable var8 = null;

                    try {
                        var9 = fun.apply(rs);
                    } catch (Throwable var56) {
                        var9 = var56;
                        var8 = var56;
                        throw var56;
                    } finally {
                        if (rs != null) {
                            if (var8 != null) {
                                try {
                                    rs.close();
                                } catch (Throwable var55) {
                                    var8.addSuppressed(var55);
                                }
                            } else {
                                rs.close();
                            }
                        }

                    }
                } catch (Throwable var58) {
                    var6 = var58;
                    throw var58;
                } finally {
                    if (stmt != null) {
                        if (var6 != null) {
                            try {
                                stmt.close();
                            } catch (Throwable var54) {
                                var6.addSuppressed(var54);
                            }
                        } else {
                            stmt.close();
                        }
                    }

                }
            } catch (Throwable var60) {
                var4 = var60;
                throw var60;
            } finally {
                if (conn != null) {
                    if (var4 != null) {
                        try {
                            conn.close();
                        } catch (Throwable var53) {
                            var4.addSuppressed(var53);
                        }
                    } else {
                        conn.close();
                    }
                }

            }

            return var9;
        } catch (Exception var62) {
            logger.error("sqlRs has error, sql: {} ", sql);
            throw new RuntimeException(var62);
        }
    }
}

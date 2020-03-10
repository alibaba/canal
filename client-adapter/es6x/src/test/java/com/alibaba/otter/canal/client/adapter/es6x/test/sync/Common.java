package com.alibaba.otter.canal.client.adapter.es6x.test.sync;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.es6x.ES6xAdapter;
import com.alibaba.otter.canal.client.adapter.es6x.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

public class Common {

    public static ES6xAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("es");
        outerAdapterConfig.setHosts(TestConstant.esHosts);
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", TestConstant.clusterName);
        outerAdapterConfig.setProperties(properties);

        ES6xAdapter esAdapter = new ES6xAdapter();
        esAdapter.init(outerAdapterConfig, null);
        return esAdapter;
    }

    public static void sqlExe(DataSource dataSource, String sql) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.execute(sql);
            conn.commit();
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    // ignore
                }
            }
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }
}

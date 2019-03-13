package com.alibaba.otter.canal.client.adapter.es.test.sync;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.es.ESAdapter;
import com.alibaba.otter.canal.client.adapter.es.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

public class Common {

    public static ESAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("es");
        outerAdapterConfig.setHosts(TestConstant.esHosts);
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", TestConstant.clusterName);
        outerAdapterConfig.setProperties(properties);

        ESAdapter esAdapter = new ESAdapter();
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

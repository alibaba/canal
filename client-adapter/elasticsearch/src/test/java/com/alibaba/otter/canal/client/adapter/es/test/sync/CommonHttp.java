package com.alibaba.otter.canal.client.adapter.es.test.sync;

import com.alibaba.otter.canal.client.adapter.es.ESHttpAdapter;
import com.alibaba.otter.canal.client.adapter.es.test.TestConstant;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class CommonHttp {

    public static ESHttpAdapter init() {
        DatasourceConfig.DATA_SOURCES.put("defaultDS", TestConstant.dataSource);

        OuterAdapterConfig outerAdapterConfig = new OuterAdapterConfig();
        outerAdapterConfig.setName("es");
        outerAdapterConfig.setHosts(TestConstant.esHttpHosts);
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", TestConstant.clusterName);
        properties.put("xpack.security.user", TestConstant.nameAndPwd);
        outerAdapterConfig.setProperties(properties);

        ESHttpAdapter esAdapter = new ESHttpAdapter();
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

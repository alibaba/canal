package com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.mybatis.spring.support.SqlSessionDaoSupport;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author agapple 2017年10月14日 上午1:05:22
 * @since 1.0.25
 */
public class MetaBaseDAO extends SqlSessionDaoSupport {

    protected boolean isH2 = false;

    protected void initTable(String tableName) throws Exception {

        try {
            DataSource dataSource = getSqlSessionFactory().getConfiguration().getEnvironment().getDataSource();
            Connection conn       = dataSource.getConnection();
            Statement  stmt       = conn.createStatement();
            String     name       = "mysql";
            isH2 = isH2(conn);
            if (isH2) {
                name = "h2";
            }
            try (InputStream input = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("ddl/" + name + "/" + tableName + ".sql")) {
                if (input == null) {
                    return;
                }

                String sql = StringUtils.join(IOUtils.readLines(input), "\n");
                stmt.execute(sql);
            }
        } catch (Throwable e) {
            logger.warn("init " + tableName + " failed", e);
        }
    }

    private boolean isH2(Connection conn) throws SQLException {
        String product = conn.getMetaData().getDatabaseProductName();
        return StringUtils.containsIgnoreCase(product, "H2");
    }
}

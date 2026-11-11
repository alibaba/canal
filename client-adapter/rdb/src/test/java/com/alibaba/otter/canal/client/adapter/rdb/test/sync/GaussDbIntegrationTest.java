package com.alibaba.otter.canal.client.adapter.rdb.test.sync;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.rdb.RdbAdapter;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

import static org.junit.Assert.*;

/**
 * Opt-in database tests. Run via src/test/docker/test-gaussdb.sh.
 */
public class GaussDbIntegrationTest {

    private static final String GAUSS_DRIVER = "com.alibaba.otter.canal.client.adapter.gaussdb.jdbc.Driver";
    private static final String TABLE = "canal_adapter_test.\"user\"";

    private DruidDataSource target;
    private DruidDataSource source;
    private RdbAdapter adapter;

    @Before
    public void init() throws Exception {
        Assume.assumeNotNull(System.getProperty("gaussdb.jdbc.url"));
        target = dataSource(GAUSS_DRIVER, System.getProperty("gaussdb.jdbc.url"), "canal_test", "Canal@Test123");
        execute(target, "CREATE SCHEMA IF NOT EXISTS canal_adapter_test");
        execute(target, "DROP TABLE IF EXISTS " + TABLE);
        execute(target, "CREATE TABLE " + TABLE
                        + " (id BIGINT PRIMARY KEY, name VARCHAR(100), amount DECIMAL(12,2), enabled BOOLEAN)");

        // MySQL syntax on the source catches accidentally using its backticks on the GaussDB target.
        source = dataSource("com.mysql.jdbc.Driver", System.getProperty("mysql.jdbc.url"), "canal_test", "Canal@Test123");
        execute(source, "DROP TABLE IF EXISTS canal_adapter_test.source_user");
        execute(source, "CREATE TABLE canal_adapter_test.source_user"
                        + " (id BIGINT PRIMARY KEY, name VARCHAR(100), amount DECIMAL(12,2), enabled BOOLEAN)");
        DatasourceConfig.DATA_SOURCES.put("gaussdbTestSource", source);

        OuterAdapterConfig config = new OuterAdapterConfig();
        config.setName("rdb");
        config.setKey("gaussdb_test");
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("jdbc.driverClassName", GAUSS_DRIVER);
        properties.put("jdbc.url", System.getProperty("gaussdb.jdbc.url"));
        properties.put("jdbc.username", "canal_test");
        properties.put("jdbc.password", "Canal@Test123");
        properties.put("threads", "1");
        config.setProperties(properties);
        Properties environment = new Properties();
        environment.setProperty("canal.conf.mode", "tcp");
        adapter = new RdbAdapter();
        adapter.init(config, environment);
    }

    @After
    public void close() {
        if (adapter != null) {
            adapter.destroy();
        }
        DatasourceConfig.DATA_SOURCES.remove("gaussdbTestSource");
        if (source != null) {
            source.close();
        }
        if (target != null) {
            target.close();
        }
    }

    @Test
    public void syncInsertUpdateDeleteAndTruncate() throws Exception {
        Map<String, Object> row = row(1L, "中文 O'Reilly", new BigDecimal("12.34"), true);
        adapter.sync(Collections.singletonList(dml("INSERT", row, null)));
        assertRow("中文 O'Reilly", "12.34", true);
        assertEquals(1L, ((Number) adapter.count("gaussdb_test.yml").get("count")).longValue());

        Map<String, Object> updated = row(1L, null, new BigDecimal("56.78"), false);
        adapter.sync(Collections.singletonList(dml("UPDATE", updated, row)));
        assertRow(null, "56.78", false);

        adapter.sync(Collections.singletonList(dml("DELETE", updated, null)));
        assertEquals(0L, ((Number) adapter.count("gaussdb_test.yml").get("count")).longValue());
        adapter.sync(Collections.singletonList(dml("INSERT", row, null)));
        adapter.sync(Collections.singletonList(dml("TRUNCATE", row, null)));
        assertEquals(0L, ((Number) adapter.count("gaussdb_test.yml").get("count")).longValue());
    }

    @Test
    public void etlFromMysqlDialectUsesTargetQuotingAndReplacesRows() throws Exception {
        execute(source, "INSERT INTO canal_adapter_test.source_user VALUES (1, '全量导入', 99.50, true)");
        execute(target, "INSERT INTO " + TABLE + " VALUES (1, 'old', 0, false)");
        EtlResult result = adapter.etl("gaussdb_test.yml", null);
        assertTrue(result.getErrorMessage(), result.getSucceeded());
        assertRow("全量导入", "99.50", true);
        result = adapter.etl("gaussdb_test.yml", null);
        assertTrue(result.getErrorMessage(), result.getSucceeded());
        assertEquals(1L, ((Number) adapter.count("gaussdb_test.yml").get("count")).longValue());
    }

    @Test
    public void postgresqlAndGaussdbConnectInSameJvm() throws Exception {
        Assume.assumeNotNull(System.getProperty("postgresql.jdbc.url"));
        try (DruidDataSource pg = dataSource("org.postgresql.Driver",
            System.getProperty("postgresql.jdbc.url"), "canal_test", "Canal@Test123")) {
            assertEquals(1, Collections.list(getClass().getClassLoader()
                .getResources("org/postgresql/Driver.class")).size());
            boolean gaussServiceFound = false;
            boolean pgServiceFound = false;
            for (Driver driver : ServiceLoader.load(Driver.class)) {
                gaussServiceFound |= GAUSS_DRIVER.equals(driver.getClass().getName());
                pgServiceFound |= "org.postgresql.Driver".equals(driver.getClass().getName());
            }
            assertTrue("Relocated GaussDB JDBC service must be discoverable", gaussServiceFound);
            assertTrue("PostgreSQL JDBC service must remain discoverable", pgServiceFound);
            assertEquals("org.postgresql.Driver", pg.getDriver().getClass().getName());
            assertEquals(GAUSS_DRIVER, target.getDriver().getClass().getName());
            assertNotSame(pg.getDriver().getClass(), target.getDriver().getClass());
            execute(pg, "CREATE TABLE IF NOT EXISTS canal_driver_test (id BIGINT PRIMARY KEY, name VARCHAR(50))");
            execute(pg, "DELETE FROM canal_driver_test");
            execute(pg, "INSERT INTO canal_driver_test VALUES (1, 'PostgreSQL')");
            try (Connection connection = pg.getConnection(); Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT name FROM canal_driver_test WHERE id=1")) {
                assertTrue(rs.next());
                assertEquals("PostgreSQL", rs.getString(1));
                assertTrue(connection.getMetaData().getDatabaseProductName().contains("PostgreSQL"));
            }
            adapter.sync(Collections.singletonList(dml("INSERT", row(1L, "GaussDB", new BigDecimal("1.00"), true), null)));
            assertRow("GaussDB", "1.00", true);
        }
    }

    private static DruidDataSource dataSource(String driver, String url, String username, String password)
                                                                                                          throws Exception {
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName(driver);
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setMaxWait(10000);
        ds.init();
        return ds;
    }

    private static void execute(DruidDataSource ds, String sql) throws Exception {
        try (Connection connection = ds.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private void assertRow(String name, String amount, boolean enabled) throws Exception {
        try (Connection connection = target.getConnection(); Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT * FROM " + TABLE + " WHERE id=1")) {
            assertTrue(rs.next());
            assertEquals(name, rs.getString("name"));
            assertEquals(new BigDecimal(amount), rs.getBigDecimal("amount"));
            assertEquals(enabled, rs.getBoolean("enabled"));
            assertFalse(rs.next());
        }
    }

    private static Map<String, Object> row(long id, String name, BigDecimal amount, boolean enabled) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("amount", amount);
        row.put("enabled", enabled);
        return row;
    }

    private static Dml dml(String type, Map<String, Object> row, Map<String, Object> old) {
        Dml dml = new Dml();
        dml.setDestination("gaussdb_test");
        dml.setDatabase("canal_adapter_test");
        dml.setTable("source_user");
        dml.setType(type);
        dml.setData(Collections.singletonList(row));
        if (old != null) {
            dml.setOld(Collections.singletonList(old));
        }
        return dml;
    }
}

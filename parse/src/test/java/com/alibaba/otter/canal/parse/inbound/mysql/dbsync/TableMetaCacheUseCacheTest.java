package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.MemoryTableMeta;
import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * 验证 #4019 的修复：useCache=false 时必须跳过 TSDB 缓存、强制从 DB 重新获取并覆盖快照
 *
 * <pre>
 * 复现路径：表从 filter 中删除后结构变化（DDL 未被 canal 捕获），重新加入 filter 后
 *           canal 从 TSDB 读取到旧快照，导致 "column size is not match"
 * </pre>
 */
public class TableMetaCacheUseCacheTest {

    private MysqlConnection connection;
    private MemoryTableMeta tableMetaTSDB;
    private TableMetaCache   tableMetaCache;

    @Before
    public void setUp() throws Exception {
        connection = Mockito.mock(MysqlConnection.class);
        // 构造函数内部会查询 rds/polarx 全局变量，默认返回空结果
        ResultSetPacket empty = new ResultSetPacket();
        empty.setFieldValues(new java.util.ArrayList<String>());
        Mockito.when(connection.query(Mockito.contains("show global variables"))).thenReturn(empty);
        tableMetaTSDB = new MemoryTableMeta();
        tableMetaCache = new TableMetaCache(connection, tableMetaTSDB);
    }

    private ResultSetPacket createTablePacket(String ddl) {
        ResultSetPacket packet = new ResultSetPacket();
        packet.setFieldValues(Arrays.asList("test", ddl));
        return packet;
    }

    @Test
    public void testUseCacheFalseForceRefreshFromDB() throws Exception {
        // 模拟 DB 返回新旧两种 DDL：第一次查 old，后续强制刷新时查 new
        String oldDDL = "create table `t_order` (`id` int primary key, `name` varchar(32));";
        String newDDL = "create table `t_order` (`id` int primary key, `name` varchar(32), `amount` decimal(10,2));";
        Mockito.when(connection.query(Mockito.contains("show create table")))
            .thenReturn(createTablePacket(oldDDL), createTablePacket(newDDL));

        // useCache=true 首次加载，从 DB 得到旧结构（2 列）
        TableMeta meta1 = tableMetaCache.getTableMeta("test", "t_order", true, null);
        Assert.assertNotNull(meta1);
        Assert.assertEquals(2, meta1.getFields().size());

        // 模拟表结构变化（DB 已变为 3 列），但 TSDB 仍是旧快照
        // 用 useCache=false 强制刷新，应跳过 TSDB 缓存、直接查 DB 并覆盖
        TableMeta meta2 = tableMetaCache.getTableMeta("test", "t_order", false, null);
        Assert.assertNotNull(meta2);
        Assert.assertEquals(3, meta2.getFields().size());
        Assert.assertNotEquals(meta1.getFields().size(), meta2.getFields().size());
    }

    @Test
    public void testUseCacheTrueStillHitsTSDB() throws Exception {
        String ddl = "create table `t_user` (`id` int primary key, `name` varchar(32));";
        Mockito.when(connection.query(Mockito.anyString())).thenReturn(createTablePacket(ddl));

        // 先 useCache=false 强制刷新，把 TSDB 填上新结构（触发一次 DB 查询）
        TableMeta meta1 = tableMetaCache.getTableMeta("test", "t_user", false, null);
        Assert.assertEquals(2, meta1.getFields().size());

        // 再 useCache=true，应命中 TSDB，不触发 DB 查询
        TableMeta meta2 = tableMetaCache.getTableMeta("test", "t_user", true, null);
        Assert.assertEquals(2, meta2.getFields().size());

        // 验证 useCache=true 没有再次查询 DB（TSDB 命中）
        Mockito.verify(connection, Mockito.times(1)).query(Mockito.contains("show create table"));
    }
}
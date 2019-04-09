package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
@Ignore
public class TableMetaCacheTest {

    @Test
    public void testSimple() throws IOException {
        MysqlConnection connection = new MysqlConnection(new InetSocketAddress("127.0.0.1", 3306), "root", "hello");
        try {
            connection.connect();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        List<ResultSetPacket> packets = connection.queryMulti("show create table test.ljh_test");
        String createDDL = null;
        if (packets.get(0).getFieldValues().size() > 0) {
            createDDL = packets.get(0).getFieldValues().get(1);
        }

        System.out.println(createDDL);

        // TableMetaCache cache = new TableMetaCache(connection);
        // TableMeta meta = cache.getTableMeta("otter1", "otter_stability1");
        // Assert.assertNotNull(meta);
        // for (FieldMeta field : meta.getFields()) {
        // System.out.println("filed :" + field.getColumnName() + " , isKey : "
        // + field.isKey() + " , isNull : "
        // + field.isNullable());
        // }
    }
}

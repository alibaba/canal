package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.taobao.tddl.dbsync.binlog.BinlogPosition;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.druid.sql.visitor.functions.Bin;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;

public class TableMetaCacheTest {

    //@Test
    //public void testSimple() {
    //
    //    MysqlConnection connection = new MysqlConnection(new InetSocketAddress("127.0.0.1", 3306), "xxxxx", "xxxxx");
    //    try {
    //        connection.connect();
    //    } catch (IOException e) {
    //        Assert.fail(e.getMessage());
    //    }
    //
    //    TableMetaCache cache = new TableMetaCache(connection);
    //    TableMeta meta = cache.getTableMeta("otter1", "otter_stability1");
    //    Assert.assertNotNull(meta);
    //    for (FieldMeta field : meta.getFields()) {
    //        System.out.println("filed :" + field.getColumnName() + " , isKey : " + field.isKey() + " , isNull : "
    //                           + field.isNullable());
    //    }
    //}
}

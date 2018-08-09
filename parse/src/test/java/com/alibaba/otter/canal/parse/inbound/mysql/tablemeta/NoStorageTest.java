package com.alibaba.otter.canal.parse.inbound.mysql.tablemeta;

import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Date;

public class NoStorageTest {
    final String DBNAME = "testdb";
    final String TBNAME = "testtb";
    final String DDL = "CREATE TABLE `testtb` (\n" +
            "   `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "   `name` varchar(2048) DEFAULT NULL,\n" +
            "   `datachange_lasttime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最晚更新时间',\n" +
            "   `otter_testcol` varchar(45) DEFAULT NULL,\n" +
            "   `otter_testcol1` varchar(45) DEFAULT NULL,\n" +
            "   `otter_testcol2` varchar(45) DEFAULT NULL,\n" +
            "   `otter_testcol3` varchar(45) DEFAULT NULL,\n" +
            "   `otter_testcol4` varchar(45) DEFAULT NULL,\n" +
            "   `otter_testcol5` varchar(45) DEFAULT NULL,\n" +
            "   PRIMARY KEY (`id`)\n" +
            " ) ENGINE=InnoDB AUTO_INCREMENT=58333898 DEFAULT CHARSET=utf8mb4";
    @Test
    public void nostorage() {
        MysqlConnection connection = new MysqlConnection(new InetSocketAddress("127.0.0.1", 3306), "root", "hello");
        TableMetaCacheWithStorage tableMetaCacheWithStorage = new TableMetaCacheWithStorage(connection, null);
        EntryPosition entryPosition = new EntryPosition();
        entryPosition.setTimestamp(new Date().getTime());
        String fullTableName = DBNAME + "." + TBNAME;
        tableMetaCacheWithStorage.apply(entryPosition, fullTableName, DDL, null);
        entryPosition.setTimestamp(new Date().getTime() + 1000L);
        TableMeta result = tableMetaCacheWithStorage.getTableMeta(DBNAME, TBNAME, false, entryPosition);
        assert result.getDdl().equalsIgnoreCase(DDL);
    }
}

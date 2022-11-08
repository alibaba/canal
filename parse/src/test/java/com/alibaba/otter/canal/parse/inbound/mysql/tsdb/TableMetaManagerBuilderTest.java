package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.util.Assert;

/**
 * @author agapple 2017年10月12日 上午10:50:00
 * @since 1.0.25
 */
public class TableMetaManagerBuilderTest {

    @Ignore
    @Test
    public void testSimple() {
        TableMetaTSDB tableMetaTSDB = TableMetaTSDBBuilder.build("test", "classpath:tsdb/mysql-tsdb.xml");
        Assert.notNull(tableMetaTSDB);
        TableMetaTSDBBuilder.destory("test");
    }
}

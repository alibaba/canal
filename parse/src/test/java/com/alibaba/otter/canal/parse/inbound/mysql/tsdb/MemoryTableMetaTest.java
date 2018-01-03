package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.otter.canal.parse.inbound.TableMeta;

/**
 * @author agapple 2017年8月1日 下午7:15:54
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/tsdb/mysql-tsdb.xml" })
public class MemoryTableMetaTest {

    @Test
    public void testSimple() throws Throwable {
        MemoryTableMeta memoryTableMeta = new MemoryTableMeta();
        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        File create = new File(dummyFile.getParent() + "/ddl", "create.sql");
        String sql = StringUtils.join(IOUtils.readLines(new FileInputStream(create)), "\n");
        memoryTableMeta.apply(null, "test", sql, null);

        TableMeta meta = memoryTableMeta.find("test", "test");
        System.out.println(meta);
        Assert.assertTrue(meta.getFieldMetaByName("ID").isKey());
    }
}

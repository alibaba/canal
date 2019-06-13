package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import javax.annotation.Resource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * @author wanshao 2017年8月2日 下午4:11:45
 * @since 3.2.5
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/tsdb/h2-tsdb.xml" })
@Ignore
public class TableMetaManagerTest {

    @Resource
    DatabaseTableMeta tableMetaManager;

    @Test
    public void testSimple() throws FileNotFoundException, IOException {
        tableMetaManager.init("test");

        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        File create = new File(dummyFile.getParent() + "/ddl", "create.sql");
        EntryPosition position = new EntryPosition("mysql-bin.001115", 139177334L, 3065927853L, 1501660815000L);
        String createSql = StringUtils.join(IOUtils.readLines(new FileInputStream(create)), "\n");
        tableMetaManager.apply(position, "tddl5_00", createSql, null);

        String alterSql = "alter table `test` add column name varchar(32) after c_varchar";
        position = new EntryPosition("mysql-bin.001115", 139177334L, 3065927854L, 1501660815000L);
        tableMetaManager.apply(position, "tddl5_00", alterSql, null);
    }
}

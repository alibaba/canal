package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import javax.annotation.Resource;

import com.taobao.tddl.dbsync.binlog.BinlogPosition;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author wanshao 2017年8月2日 下午4:11:45
 * @since 3.2.5
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/dal-dao.xml" })
public class TableMetaManagerTest {

    @Resource
    TableMetaManager tableMetaManager;

    @Test
    public void testSimple() throws FileNotFoundException, IOException {



        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        File create = new File(dummyFile.getParent() + "/ddl", "create.sql");
        BinlogPosition position = BinlogPosition.parseFromString("001115:0139177334#3065927853.1501660815000");
        String createSql = StringUtils.join(IOUtils.readLines(new FileInputStream(create)), "\n");
        tableMetaManager.apply(position, "tddl5_00", createSql);

        String alterSql = "alter table `test` add column name varchar(32) after c_varchar";
        position = BinlogPosition.parseFromString("001115:0139177334#3065927853.1501660816000");
        tableMetaManager.apply(position, "tddl5_00", alterSql);

    }
}

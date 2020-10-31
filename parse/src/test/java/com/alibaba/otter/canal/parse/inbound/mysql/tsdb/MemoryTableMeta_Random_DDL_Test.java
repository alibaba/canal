package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.druid.sql.repository.Schema;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.google.common.collect.Lists;

/**
 * @author agapple 2017年8月1日 下午7:15:54
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/tsdb/h2-tsdb.xml" })
public class MemoryTableMeta_Random_DDL_Test {

    @Test
    public void test_database() throws Throwable {
        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        int number = 39;
        for (int i = 1; i <= number; i++) {
            File sourceFile = new File(dummyFile.getParent() + "/ddl/table", "test_" + i + ".sql");
            String sourceSql = StringUtils.join(IOUtils.readLines(new FileInputStream(sourceFile)), "\n");
            MemoryTableMeta source = new MemoryTableMeta();
            source.apply(null, "test", sourceSql, null);

            File targetFile = new File(dummyFile.getParent() + "/ddl/table", "mysql_" + i + ".sql");
            String targetSql = StringUtils.join(IOUtils.readLines(new FileInputStream(targetFile)), "\n");
            MemoryTableMeta target = new MemoryTableMeta();
            target.apply(null, "test", targetSql, null);

            compareTableMeta(i, source, target);
        }
    }

    @Test
    public void test_table() throws Throwable {
        URL url = Thread.currentThread().getContextClassLoader().getResource("dummy.txt");
        File dummyFile = new File(url.getFile());
        int number = 80;
        for (int i = 1; i <= number; i++) {
            try {
                File sourceFile = new File(dummyFile.getParent() + "/ddl/alter", "test_" + i + ".sql");
                String sourceSql = StringUtils.join(IOUtils.readLines(new FileInputStream(sourceFile)), "\n");
                MemoryTableMeta source = new MemoryTableMeta();
                source.apply(null, "test", sourceSql, null);

                File targetFile = new File(dummyFile.getParent() + "/ddl/alter", "mysql_" + i + ".sql");
                String targetSql = StringUtils.join(IOUtils.readLines(new FileInputStream(targetFile)), "\n");
                MemoryTableMeta target = new MemoryTableMeta();
                target.apply(null, "test", targetSql, null);

                compareTableMeta(i, source, target);
            } catch (Throwable e) {
                Assert.fail("case : " + i + " failed by : " + e.getMessage());
            }
        }
    }

    private void compareTableMeta(int num, MemoryTableMeta source, MemoryTableMeta target) {
        List<String> tableNames = Lists.newArrayList();
        for (Schema schema : source.getRepository().getSchemas()) {
            tableNames.addAll(schema.showTables());
        }

        for (String table : tableNames) {
            TableMeta sourceMeta = source.find("test", table);
            TableMeta targetMeta = target.find("test", table);
            boolean result = DatabaseTableMeta.compareTableMeta(sourceMeta, targetMeta);
            if (!result) {
                Assert.fail(sourceMeta.toString() + " vs " + targetMeta.toString());
            }
        }
    }
}

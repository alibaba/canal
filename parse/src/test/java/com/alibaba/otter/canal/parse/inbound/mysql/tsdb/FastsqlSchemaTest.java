package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.druid.sql.repository.SchemaObject;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;

public class FastsqlSchemaTest {

    @Test
    public void testSimple() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql1 = "CREATE TABLE `table_x1` ( `id` bigint(20) NOT NULL AUTO_INCREMENT, "
                      + "`key1` longtext NOT NULL COMMENT 'key1', `value1` longtext NOT NULL COMMENT 'value1', PRIMARY KEY (`id`) )"
                      + "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        String sql2 = " CREATE TABLE IF NOT EXISTS `table_x1` ( `id` bigint(20) NOT NULL AUTO_INCREMENT,"
                      + "`key1` longtext NOT NULL COMMENT 'key1',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        repository.console(sql1);
        repository.console(sql2);
        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("table_x1");
        System.out.println(table.getStatement().toString());
        Assert.assertTrue(table.findColumn("value1") != null);
    }

    @Test
    public void test_block_format() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = " CREATE TABLE `parent` (`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
                     + "`created_at` timestamp NULL DEFAULT NULL, " + "PRIMARY KEY (`id`)"
                     + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 BLOCK_FORMAT=ENCRYPTED";
        repository.console(sql);
        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("parent");
        System.out.println(table.getStatement().toString());
        Assert.assertTrue(table.findColumn("id") != null);
    }

    @Test
    public void test_json_index() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = " CREATE TABLE `articles` ( `article_id` bigint NOT NULL AUTO_INCREMENT,"
                     + " `tags` json DEFAULT NULL, PRIMARY KEY (`article_id`),"
                     + " KEY `articles_tags` ((cast(json_extract(`tags`,_utf8mb4'$[*]') as char(40) array)))"
                     + ") ENGINE=InnoDB AUTO_INCREMENT=1054 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
        repository.console(sql);
        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("articles");
        System.out.println(table.getStatement().toString());
        Assert.assertTrue(table.findColumn("article_id") != null);
    }

    @Test
    public void test_invisible() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = " CREATE TABLE `proposal_order_info` (`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
                     + "`created_at` timestamp NULL DEFAULT NULL, " + "PRIMARY KEY (`id`) , "
                     + "KEY `idx_create_time` (`created_at`) /*!80000 INVISIBLE */"
                     + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 BLOCK_FORMAT=ENCRYPTED";
        repository.console(sql);
        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("proposal_order_info");
        System.out.println(table.getStatement().toString());
        Assert.assertTrue(table.findColumn("id") != null);
    }

    @Test
    public void test_persistent() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = " create table example_vc_tbl( c1 int not null auto_increment primary key,"
                     + "c2 varchar(70), vc1 int as (length(c2)) virtual,"
                     + "DIM_SUM varchar(128) AS (MD5(UPPER(CONCAT(c2, c1)))) PERSISTENT)";
        repository.console(sql);
        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("example_vc_tbl");
        System.out.println(table.getStatement().toString());
        Assert.assertTrue(table.findColumn("c1") != null);
    }

    @Test
    public void test_primaryKey() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        {
            String sql1 = "CREATE TABLE test ( id NOT NULL, name varchar(32) ) ENGINE=InnoDB; ";
            String sql2 = " ALTER TABLE test add primary key(id);";
            repository.console(sql1);
            String rs = repository.console(sql2);
            System.out.println(rs);
            repository.setDefaultSchema("test");
            SchemaObject table = repository.findTable("test");
            Assert.assertTrue(table.findColumn("id").isOnlyPrimaryKey());
        }

        {
            String sql1 = "CREATE TABLE test ( id NOT NULL, name varchar(32) ) ENGINE=InnoDB; ";
            String sql2 = "ALTER TABLE test MODIFY id bigint AUTO_INCREMENT PRIMARY KEY; ";
            repository.console(sql1);
            repository.console(sql2);
            repository.setDefaultSchema("test");
            SchemaObject table = repository.findTable("test");
            Assert.assertTrue(table.findColumn("id").isOnlyPrimaryKey());
            Assert.assertTrue(table.findColumn("id").isAutoIncrement());
        }
    }

    @Test
    public void test_partition_table() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql1 = "create table test (\n" + " id int not null AUTO_INCREMENT primary key,\n"
                      + " name varchar(32) \n" + " )\n" + " partition by range(id) (\n"
                      + " partition p1 values less than (10),\n" + " partition px values less than MAXVALUE\n"
                      + " );";
        String sql2 = "alter table test add partition ( partition 2 VALUES LESS THAN (738552) ENGINE = InnoDB, PARTITION pmax VALUES LESS THAN MAXVALUE ENGINE = InnoDB)";
        repository.console(sql1);
        repository.console(sql2);
        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("test");
        Assert.assertTrue(table != null);
    }

    @Test
    public void test_mariadb_aria() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql1 = "CREATE TABLE test (\n" + "db_name varchar(64) COLLATE utf8_bin NOT NULL,\n"
                      + "table_name varchar(64) COLLATE utf8_bin NOT NULL,\n"
                      + "column_name varchar(64) COLLATE utf8_bin NOT NULL,\n"
                      + "min_value varbinary(255) DEFAULT NULL,\n" + "max_value varbinary(255) DEFAULT NULL,\n"
                      + "nulls_ratio decimal(12,4) DEFAULT NULL,\n" + "avg_length decimal(12,4) DEFAULT NULL,\n"
                      + "avg_frequency decimal(12,4) DEFAULT NULL,\n" + "hist_size tinyint(3) unsigned DEFAULT NULL,\n"
                      + "hist_type enum('SINGLE_PREC_HB','DOUBLE_PREC_HB') COLLATE utf8_bin DEFAULT NULL,\n"
                      + "histogram varbinary(255) DEFAULT NULL,\n" + "PRIMARY KEY (db_name,table_name,column_name)\n"
                      + ") ENGINE=Aria DEFAULT CHARSET=utf8 COLLATE=utf8_bin PAGE_CHECKSUM=1 TRANSACTIONAL=0";
        repository.console(sql1);
        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("test");
        Assert.assertTrue(table != null);
    }

    @Test
    public void test_polardb_x() throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("test");

        String sql1 = "CREATE TABLE `test1` (\n" + "  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                      + "  `serialNo` varchar(64) CHARACTER SET utf8mb4 NOT NULL DEFAULT '',\n"
                      + "  `user_id` int(11) DEFAULT NULL COMMENT '用户id',\n" + "  PRIMARY KEY (`id`)\n"
                      + ") ENGINE = InnoDB  PARTITION BY KEY(`tenant_id`,`id`)\n" + "PARTITIONS 21 tablegroup = `tg_p_msg`";
        repository.console(sql1);
        SchemaObject table = repository.findTable("test1");
        Assert.assertTrue(table != null);


        String sql2 = "CREATE TABLE `test2` (\n" + "  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                      + "  `serialNo` varchar(64) CHARACTER SET utf8mb4 NOT NULL DEFAULT '',\n"
                      + "  `user_id` int(11) DEFAULT NULL COMMENT '用户id',\n" + "  PRIMARY KEY (`id`)\n"
                      + ") ENGINE = InnoDB single";
        repository.console(sql2);
        table = repository.findTable("test2");
        Assert.assertTrue(table != null);


        String sql3 = "CREATE TABLE `test3` (\n" + "  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                      + "  `serialNo` varchar(64) CHARACTER SET utf8mb4 NOT NULL DEFAULT '',\n"
                      + "  `user_id` int(11) DEFAULT NULL COMMENT '用户id',\n" + "  PRIMARY KEY (`id`)\n"
                      + ") ENGINE = InnoDB locality = 'dn=polardbx-ng28-dn-1,polardbx-ng28-dn-2'";
        repository.console(sql3);
        table = repository.findTable("test3");
        Assert.assertTrue(table != null);

        String sql4 = "CREATE TABLE test4(\n" + " order_id int AUTO_INCREMENT primary key,\n"
                      + " customer_id int,\n" + " country varchar(64),\n" + " city varchar(64),\n"
                      + " order_time datetime not null)\n" + "PARTITION BY LIST COLUMNS(country,city)\n" + "(\n"
                      + "  PARTITION p1 VALUES IN (('China','Shanghai')) LOCALITY = 'dn=polardbx-ng28-dn-2',\n"
                      + "  PARTITION p2 VALUES IN (('China','Beijing')) LOCALITY = 'dn=polardbx-ng28-dn-2',\n"
                      + "  PARTITION p3 VALUES IN (('China','Hangzhou')) ,\n"
                      + "  PARTITION p4 VALUES IN (('China','Nanjing')) ,\n"
                      + "  PARTITION p5 VALUES IN (('China','Guangzhou')) ,\n"
                      + "  PARTITION p6 VALUES IN (('China','Shenzhen')) ,\n"
                      + "  PARTITION p7 VALUES IN (('China','Wuhan')) ,\n"
                      + "  PARTITION p8 VALUES IN (('America','New York'))\n"
                      + ") LOCALITY = 'dn=polardbx-ng28-dn-0,polardbx-ng28-dn-1';";
        repository.console(sql4);
        table = repository.findTable("test4");
        Assert.assertTrue(table != null);
    }
}

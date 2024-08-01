package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.repository.Schema;
import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

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

        String sql5 = "CREATE TABLE `test5` (\n" + "\t`Id` varchar(32) NOT NULL COMMENT '',\n"
                      + "\t`ExitId` varchar(32) NOT NULL COMMENT '',\n"
                      + "\t`CreateTime` datetime NOT NULL COMMENT '创建时间',\n"
                      + "\t`archive_date` datetime NOT NULL DEFAULT '2099-01-01 00:00:00',\n"
                      + "\tPRIMARY KEY (`Id`, `archive_date`),\n"
                      + "\tGLOBAL INDEX `g_i_id` (`id`) COVERING (`ExitId`) \n" + "\t\tPARTITION BY KEY(`Id`)\n"
                      + "\t\tPARTITIONS 16,\n" + "\tKEY `ExitId` USING BTREE (`ExitId`),\n"
                      + "\tKEY `CreateTime` (`CreateTime`),\n" + "\tKEY `i_id_ExitId` USING BTREE (`Id`, `ExitId`),\n"
                      + "\tKEY `auto_shard_key_ExitId_id` USING BTREE (`ExitId`, `Id`)\n"
                      + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n" + "PARTITION BY KEY(`ExitId`,`Id`)\n"
                      + "PARTITIONS 16\n" + "LOCAL PARTITION BY RANGE (archive_date)\n" + "INTERVAL 1 MONTH\n"
                      + "EXPIRE AFTER 27\n" + "PRE ALLOCATE 2\n" + "PIVOTDATE NOW()";
        repository.console(sql5);
        table = repository.findTable("test5");
        Assert.assertTrue(table != null);
    }

    @Test
    public void test_escapse_sql() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("test");
        String sql = "CREATE TABLE test1 (\n" + "\tid int(11) NOT NULL AUTO_INCREMENT,\n"
                     + "\tcluster_id int(11) NOT NULL COMMENT '集群id',\n"
                     + "\tcomponent_id int(11) NOT NULL COMMENT '组件id',\n"
                     + "\tcomponent_type_code tinyint(1) NOT NULL COMMENT '组件类型',\n"
                     + "\ttype varchar(128) COLLATE utf8_bin NOT NULL COMMENT '配置类型',\n"
                     + "\trequired tinyint(1) NOT NULL COMMENT 'true/false',\n"
                     + "\t`key` varchar(256) COLLATE utf8_bin NOT NULL COMMENT '配置键',\n"
                     + "\t`value` text COLLATE utf8_bin COMMENT '默认配置项',\n"
                     + "\t`values` varchar(512) COLLATE utf8_bin DEFAULT NULL COMMENT '可配置项',\n"
                     + "\tdependencyKey varchar(256) COLLATE utf8_bin DEFAULT NULL COMMENT '依赖键',\n"
                     + "\tdependencyValue varchar(256) COLLATE utf8_bin DEFAULT NULL COMMENT '依赖值',\n"
                     + "\t`desc` varchar(512) COLLATE utf8_bin DEFAULT NULL COMMENT '描述',\n"
                     + "\tgmt_create datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n"
                     + "\tgmt_modified datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',\n"
                     + "\tis_deleted tinyint(1) NOT NULL DEFAULT '0' COMMENT '0正常 1逻辑删除',\n" + "\tPRIMARY KEY (id),\n"
                     + "\tKEY index_cluster_id (cluster_id),\n" + "\tKEY index_componentId (component_id)\n"
                     + ") ENGINE = InnoDB AUTO_INCREMENT = 1140 CHARSET = utf8 COLLATE = utf8_bin;";
        repository.console(sql);
        SchemaObject table = repository.findTable("test1");
        Assert.assertTrue(table != null);
        // 应用到新的schema
        Schema schema = repository.findSchema("test");
        StringBuilder data = new StringBuilder(4 * 1024);
        for (String tableIn : schema.showTables()) {
            SchemaObject schemaObject = schema.findTable(tableIn);

            SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(data, DbType.mysql);
            visitor.config(VisitorFeature.OutputNameQuote, true);

            schemaObject.getStatement().accept(visitor);
            data.append("; \n");
        }

        repository.setDefaultSchema("test_new");
        repository.console(data.toString());
        table = repository.findTable("test1");
        Assert.assertTrue(table != null);

        // 打印新的schema的内容
        schema = repository.findSchema("test_new");
        data = new StringBuilder(4 * 1024);
        for (String tableIn : schema.showTables()) {
            SchemaObject schemaObject = schema.findTable(tableIn);

            SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(data, DbType.mysql);
            visitor.config(VisitorFeature.OutputNameQuote, true);

            schemaObject.getStatement().accept(visitor);
            data.append("; \n");
        }

        System.out.println(data.toString());
    }


    @Test
    public void test_escapse_sql2() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("test");
        String sql = "CREATE TABLE test1 (\n" + "id int(11) NOT NULL AUTO_INCREMENT,\n"
                     + "uid int(11) NOT NULL DEFAULT '0' COMMENT '用户ID',\n"
                     + "`from` tinyint(3) NOT NULL DEFAULT '0' COMMENT '来源 1QQ 2微信 3微博',\n"
                     + "access_code varchar(20) NOT NULL DEFAULT '' COMMENT '临时code',\n"
                     + "access_token varchar(200) NOT NULL DEFAULT '' COMMENT '授权token',\n"
                     + "expires_in int(11) NOT NULL DEFAULT '0' COMMENT '过期时间',\n"
                     + "refresh_token varchar(200) NOT NULL DEFAULT '' COMMENT '刷新token',\n"
                     + "openid varchar(255) NOT NULL DEFAULT '' COMMENT '授权用户唯一标识',\n"
                     + "scope varchar(60) NOT NULL COMMENT '授权范围',\n"
                     + "status tinyint(3) NOT NULL DEFAULT '1' COMMENT '1 ',\n"
                     + "json_info text NOT NULL COMMENT '个人信息',\n"
                     + "create_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '创建时间',\n"
                     + "unionid varchar(128) NOT NULL DEFAULT '' COMMENT 'UnionID',\n"
                     + "session_key varchar(128) NOT NULL DEFAULT '' COMMENT '小程序session_key',\n"
                     + "PRIMARY KEY (id),\n" + "KEY uid (uid),\n"
                     + "KEY idx_openid_from_status (openid, `from`, status),\n" + "KEY idx_access_token (access_token),\n"
                     + "KEY idx_uid_from_status (uid, `from`, status),\n"
                     + "KEY idx_unionid_from_status (unionid, `from`, status)\n"
                     + ") ENGINE = InnoDB AUTO_INCREMENT = 7831300 CHARSET = utf8 COMMENT '用户第三方登录表'";
        repository.console(sql);
        SchemaObject table = repository.findTable("test1");
        Assert.assertTrue(table != null);

        // 应用到新的schema
        Schema schema = repository.findSchema("test");
        StringBuilder data = new StringBuilder(4 * 1024);
        for (String tableIn : schema.showTables()) {
            SchemaObject schemaObject = schema.findTable(tableIn);

            SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(data, DbType.mysql);
            visitor.config(VisitorFeature.OutputNameQuote, true);

            schemaObject.getStatement().accept(visitor);
            data.append("; \n");
        }

        repository.setDefaultSchema("test_new");
        repository.console(data.toString());
        table = repository.findTable("test1");
        Assert.assertTrue(table != null);

        // 打印新的schema的内容
        schema = repository.findSchema("test_new");
        data = new StringBuilder(4 * 1024);
        for (String tableIn : schema.showTables()) {
            SchemaObject schemaObject = schema.findTable(tableIn);

            SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(data, DbType.mysql);
            visitor.config(VisitorFeature.OutputNameQuote, true);

            schemaObject.getStatement().accept(visitor);
            data.append("; \n");
        }

        System.out.println(data.toString());
    }


    @Test
    public void test_function_index () throws Throwable {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("test");
        String sql = "CREATE TABLE test1 (\n" + "    id INT AUTO_INCREMENT PRIMARY KEY,\n"
                     + "    owner_id INT NOT NULL,\n" + "    code VARCHAR(100) NOT NULL,\n"
                     + "    UNIQUE KEY uk_owner_id_upper_code (owner_id, (upper(code)))\n" + ");";
        repository.console(sql);
        SchemaObject table = repository.findTable("test1");
        Assert.assertTrue(table != null);
    }

}

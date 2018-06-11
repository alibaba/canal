package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import com.alibaba.fastsql.util.JdbcConstants;

/**
 * @author agapple 2018年6月7日 下午5:36:13
 * @since 3.1.9
 */
public class FastsqlSchemaTest {

    @Test
    public void testSimple() throws FileNotFoundException, IOException {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "create table yushitai_test.card_record ( id bigint auto_increment) auto_increment=256 "
                     + "; alter table yushitai_test.card_record add column customization_id bigint unsigned NOT NULL COMMENT 'TEST' ;"
                     + "; rename table yushitai_test.card_record to yushitai_test._card_record_del;";
        repository.console(sql);

        repository.setDefaultSchema("yushitai_test");
        SchemaObject table = repository.findTable("_card_record_del");
        System.out.println(table.getStatement().toString());
    }
}

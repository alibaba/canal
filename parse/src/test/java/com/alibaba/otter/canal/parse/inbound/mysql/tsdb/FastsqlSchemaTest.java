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
        String sql = "create table yushitai_test.card_record ( id bigint auto_increment, name varchar(32) DEFAULT NULL) auto_increment=256 "
                     + "alter table yushitai_test.card_record add index index_name(name) ;"
                     + "alter table yushitai_test.card_record add index index_name(name) ;"
                     + "alter table yushitai_test.card_record add Constraint pk_id PRIMARY KEY (id);"
                     + "alter table yushitai_test.card_record add Constraint pk_id PRIMARY KEY (id);"
                     + "alter table yushitai_test.card_record add Constraint UNIQUE index uk_name(name);"
                     + "alter table yushitai_test.card_record add Constraint UNIQUE index uk_name(name);";
        repository.console(sql);

        repository.setDefaultSchema("yushitai_test");
        SchemaObject table = repository.findTable("card_record");
        System.out.println(table.getStatement().toString());
    }
}

package com.alibaba.otter.canal.admin.config;

import javax.sql.DataSource;

import io.ebean.EbeanServer;
import org.apache.commons.dbutils.QueryRunner;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    @Bean
    public QueryRunner queryRunner(DataSource dataSource) {
        return new QueryRunner(dataSource);
    }

//    @Bean("ebeanServer")
//    public FactoryBean<EbeanServer> ebeanServer(DataSource dataSource) {
//        return new EbeanServerFactory(dataSource);
//    }
}

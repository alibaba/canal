package com.alibaba.otter.canal.admin.dao;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.Person;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.javalite.activejdbc.Base;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.SQLException;

@Repository
public class CanalConfigDao {
    @Autowired
    private DataSource dataSource;
//    public void open() {
//        Base.open(dataSource);
//    }

    @PostConstruct
    public void init() {
//        CanalConfig canalConfig = new CanalConfig();
//        canalConfig.setId(1L);
        UpdateContent(1L);
    }

    public void UpdateContent(Long id) {
        QueryRunner runner = new QueryRunner(dataSource);
        try {
            CanalConfig canalConfig = runner.query("select * from canal_config where id=1", new BeanHandler<>(CanalConfig.class));
            canalConfig = canalConfig;
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        Base.open(dataSource);
//        Person.where("id=?", id);
//        Base.close();
    }
}

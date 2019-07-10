package com.alibaba.otter.canal.admin.dao;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.Date;

@Repository
public class CanalConfigDao {
    @Autowired
    private QueryRunner runner;

    @PostConstruct
    public void init() {
        try {
            CanalConfig canalConfig = findById(1L);
            canalConfig = canalConfig;
            canalConfig.setContent(canalConfig.getContent()+"   xxx");
            updateContent(canalConfig);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public CanalConfig findById(Long id) throws SQLException {
        String sql = "select id,name,content,modified_time as modifiedTime" +
                " from canal_config where id=?";
        return runner.query(sql, new BeanHandler<>(CanalConfig.class), id);
    }

    public void updateContent(CanalConfig canalConfig) throws SQLException {
        String sql = "update canal_config set content=?, modified_time=? where id=?";
        runner.update(sql, canalConfig.getContent(), new Date(), canalConfig.getId());
    }
}

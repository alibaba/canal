package com.alibaba.otter.canal.admin.dao;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.util.Date;

@Repository
public class CanalConfigDao {

    @Autowired
    private QueryRunner runner;

    public CanalConfig findById(Long id) {
        try {
            String sql = "select id,name,content,modified_time as modifiedTime" + " from canal_config where id=?";
            return runner.query(sql, new BeanHandler<>(CanalConfig.class), id);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public int updateContent(CanalConfig canalConfig){
        try {
            String sql = "update canal_config set content=?, modified_time=? where id=?";
            return runner.update(sql, canalConfig.getContent(), new Date(), canalConfig.getId());
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}

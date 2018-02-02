package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.util.List;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDAO;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.dao.MetaHistoryDO;

/**
 * Created by wanshao Date: 2017/9/20 Time: 下午5:00
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/tsdb/mysql-tsdb.xml" })
public class MetaHistoryDAOTest {

    @Resource
    MetaHistoryDAO metaHistoryDAO;

    @Test
    public void testSimple() {
        List<MetaHistoryDO> metaHistoryDOList = metaHistoryDAO.findByTimestamp("test", 0L, 0L);
        for (MetaHistoryDO metaHistoryDO : metaHistoryDOList) {
            System.out.println(metaHistoryDO.getId());
        }
    }

}

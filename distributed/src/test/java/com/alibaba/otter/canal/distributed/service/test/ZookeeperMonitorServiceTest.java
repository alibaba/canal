package com.alibaba.otter.canal.distributed.service.test;

import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.distributed.service.MonitorService;
import com.alibaba.otter.canal.distributed.service.zkmonitor.ZookeeperMonitorService;

@Ignore
public class ZookeeperMonitorServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMonitorServiceTest.class);

    @Test
    public void testMonitor() throws Exception {
        Thread thread1 = new Thread(() -> test(1));

        Thread thread2 = new Thread(() -> test(2));

        thread1.start();
        Thread.sleep(2000L);
        thread2.start();

        thread1.join();
        thread2.join();
    }

    private void test(int i) {
        try {
            ZkClientx zkClientx = new ZkClientx("127.0.0.1:2181");

            MonitorService monitorService = new ZookeeperMonitorService(zkClientx);

            monitorService.init("test" + i);

            List<String> nodes = monitorService.listen(nodeList -> {
                System.out.println("Thread" + i + "changed trigger, left nodes: " + nodeList);
                if (i == 1) {
                    Assert.assertEquals(nodeList.size(), 2); // left test1,test2
                } else if (i == 2) {
                    Assert.assertEquals(nodeList.size(), 1); // left test2
                }
            });
            System.out.println("Thread" + i + " init nodes: " + nodes);
            if (i == 1) {
                Assert.assertEquals(nodes.size(), 1);
            } else if (i == 2) {
                Assert.assertEquals(nodes.size(), 2);
            }

            Thread.sleep(5000L);
            monitorService.destroy();
            zkClientx.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}

package com.alibaba.otter.canal.distributed.service.test;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.distributed.service.ExecutionService;
import com.alibaba.otter.canal.distributed.service.chash.ConsistentHashDistributedService;

@Ignore
public class ConsistentHashDistributedServiceTest {

    private static final Logger logger       = LoggerFactory.getLogger(ConsistentHashDistributedServiceTest.class);

    private static final String TEST_ZK_ADDR = "127.0.0.1:2181";
    private static final String TEST_NODES   = "192.168.1.1{0}:11121";

    private CyclicBarrier       cb           = new CyclicBarrier(2);

    /**
     * 测试增加节点的情况
     */
    @Test
    public void testAddNode() throws Exception {
        Thread thread1 = new Thread(() -> test4AddNode(1));

        Thread thread2 = new Thread(() -> test4AddNode(2));

        thread1.start();
        Thread.sleep(2000L);
        thread2.start();

        thread1.join();
        thread2.join();
    }

    private void test4AddNode(int i) {
        try {
            List<String> instances = Arrays.asList("test1",
                "test2",
                "test3",
                "test4",
                "test5",
                "test6",
                "test7",
                "test8",
                "test9",
                "test10",
                "test11",
                "test12",
                "test13",
                "test14",
                "test15",
                "test16",
                "test17",
                "test18",
                "test19",
                "test20");

            ZkClientx zkClientx = new ZkClientx(TEST_ZK_ADDR);

            ConsistentHashDistributedService consistentHashDistributedService = new ConsistentHashDistributedService(
                zkClientx,
                instances,
                new ExecutionService() {

                    @Override
                    public void start(String instance) {
                        System.out.println("Thread" + i + " Start instance: " + instance);
                    }

                    @Override
                    public void stop(String instance) {
                        System.out.println("Thread" + i + " Stop instance: " + instance);
                    }
                });

            consistentHashDistributedService.registerNode(MessageFormat.format(TEST_NODES, i));
            consistentHashDistributedService.distribute();
            Thread.sleep(8000L);
            consistentHashDistributedService.releaseNode();
            zkClientx.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 测试增加实例的情况
     */
    @Test
    public void testAddInstance() throws Exception {
        Thread thread1 = new Thread(() -> test4AddInstance(1, "add"));

        Thread thread2 = new Thread(() -> test4AddInstance(2, "add"));

        thread1.start();
        Thread.sleep(2000L);
        thread2.start();
        Thread.sleep(3000L);

        thread1.join();
        thread2.join();
    }

    /**
     * 测试删除实例的情况
     */
    @Test
    public void testRemoveInstance() throws Exception {
        Thread thread1 = new Thread(() -> test4AddInstance(1, "remove"));

        Thread thread2 = new Thread(() -> test4AddInstance(2, "remove"));

        thread1.start();
        Thread.sleep(2000L);
        thread2.start();
        Thread.sleep(3000L);

        thread1.join();
        thread2.join();
    }

    private void test4AddInstance(int i, String option) {
        try {
            List<String> instances = Arrays.asList("test1",
                "test2",
                "test3",
                "test4",
                "test5",
                "test6",
                "test7",
                "test8",
                "test9",
                "test10",
                "test11",
                "test12",
                "test13",
                "test14",
                "test15",
                "test16",
                "test17",
                "test18",
                "test19",
                "test20");

            ZkClientx zkClientx = new ZkClientx(TEST_ZK_ADDR);

            ConsistentHashDistributedService consistentHashDistributedService = new ConsistentHashDistributedService(
                zkClientx,
                instances,
                new ExecutionService() {

                    @Override
                    public void start(String instance) {
                        System.out.println("Thread" + i + " Start instance: " + instance);
                    }

                    @Override
                    public void stop(String instance) {
                        System.out.println("Thread" + i + " Stop instance: " + instance);
                    }
                });

            consistentHashDistributedService.registerNode(MessageFormat.format(TEST_NODES, i));
            consistentHashDistributedService.distribute();

            cb.await(); // 等待所有节点都启动完成

            if ("add".equals(option)) {
                consistentHashDistributedService.addInstance("test21"); // 添加一个节点
            } else if ("remove".equals(option)) {
                consistentHashDistributedService.removeInstance("test20"); // 删除一个节点
            }

            if (i == 1) {
                Thread.sleep(3000L);
            } else if (i == 2) {
                Thread.sleep(5000L);
            }

            consistentHashDistributedService.releaseNode();
            zkClientx.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}

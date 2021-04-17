package com.alibaba.otter.canal.distributed.service.test;

import java.util.*;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.distributed.service.chash.util.ConsistentHash;
import com.alibaba.otter.canal.distributed.service.chash.util.HashService;

public class ConsistentHashTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashTest.class);

    @Test
    public void testCHash() {
        List<String> nodes = Arrays.asList("192.168.1.11:11121", "192.168.1.12:11121", "192.168.1.13:11121");
        ConsistentHash<String> consistentHash = new ConsistentHash<>(new HashService(), 5000, nodes);
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
        Map<String, List<String>> nodeInstanceMap = new LinkedHashMap<>();
        for (String instance : instances) {
            String node = consistentHash.get(instance);
            List<String> instanceGroup = nodeInstanceMap.computeIfAbsent(node, v -> new ArrayList<>());
            instanceGroup.add(instance);
        }
        logger.info(nodeInstanceMap.toString());
        Assert.assertEquals(nodeInstanceMap.size(), 3); // 3个节点都能分配到实例

        Map<String, List<String>> nodeInstanceMap2 = new LinkedHashMap<>();
        for (String instance : instances) {
            String node = consistentHash.get(instance);
            List<String> instanceGroup = nodeInstanceMap2.computeIfAbsent(node, v -> new ArrayList<>());
            instanceGroup.add(instance);
        }

        Assert.assertEquals(nodeInstanceMap, nodeInstanceMap2); // 两次分片结果保持一致
    }
}

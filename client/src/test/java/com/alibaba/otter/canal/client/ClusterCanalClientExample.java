package com.alibaba.otter.canal.client;

public class ClusterCanalClientExample extends AbstractCanalClientExample {

    public static void main(String args[]) {
        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");

        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector("10.20.144.51:2181", "example", "", "");

        ClusterCanalClientExample clientTest = new ClusterCanalClientExample();
        clientTest.testCanal(connector);
    }
}

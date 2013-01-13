package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.protocol.Message;

public class ClusterCanalClientExample {

    public static void main(String args[]) {
        // 创建链接
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");
        CanalConnector connector = CanalConnectors.newClusterConnector("10.20.144.51:2181", "stability_test", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe("otter1.otter_stabilty1");
            connector.rollback();
            while (emptyCount < 10) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }
}

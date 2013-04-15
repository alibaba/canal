package com.alibaba.otter.canal.client;

import java.net.InetSocketAddress;

import com.alibaba.otter.canal.common.utils.AddressUtils;

public class SimpleCanalClientExample extends AbstractCanalClientExample {

    public static void main(String args[]) {
        // 根据ip，直接创建链接，无HA的功能
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                                                                                            11111), "example", "", "");

        SimpleCanalClientExample clientTest = new SimpleCanalClientExample();
        clientTest.testCanal(connector);
    }

}

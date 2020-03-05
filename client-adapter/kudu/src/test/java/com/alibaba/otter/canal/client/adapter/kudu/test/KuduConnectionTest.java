package com.alibaba.otter.canal.client.adapter.kudu.test;

import java.util.Arrays;
import java.util.List;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.junit.Test;

public class KuduConnectionTest {

    @Test
    public void test01() {
        List<String> masterList = Arrays.asList("10.6.36.102:7051,10.6.36.187:7051,10.6.36.229:7051".split(","));
        KuduClient kuduClient = new KuduClient.KuduClientBuilder(masterList).defaultOperationTimeoutMs(60000)
            .defaultSocketReadTimeoutMs(30000)
            .defaultAdminOperationTimeoutMs(60000)
            .build();
        try {
            List<String> tablesList = kuduClient.getTablesList().getTablesList();
            System.out.println(tablesList.toString());
            KuduTable web_white_list = kuduClient.openTable("web_white_list");

        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}

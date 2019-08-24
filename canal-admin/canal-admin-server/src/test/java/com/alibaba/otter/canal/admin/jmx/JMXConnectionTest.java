package com.alibaba.otter.canal.admin.jmx;

import org.junit.Test;

public class JMXConnectionTest {

    @Test
    public void testSimple() {

        JMXConnection jmxConnection = new JMXConnection("127.0.0.1", 11113);
        try {
            CanalServerMXBean canalServerMXBean = jmxConnection.getCanalServerMXBean();
            System.out.println(canalServerMXBean.getStatus());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jmxConnection.close();
        }
    }
}

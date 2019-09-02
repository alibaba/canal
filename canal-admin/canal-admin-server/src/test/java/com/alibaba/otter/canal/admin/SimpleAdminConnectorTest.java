package com.alibaba.otter.canal.admin;

import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.admin.connector.SimpleAdminConnector;

@Ignore
public class SimpleAdminConnectorTest {

    @Test
    public void testSimple() {
        SimpleAdminConnector connector = new SimpleAdminConnector("127.0.0.1", 11110, "admin", "admin");
        connector.connect();
        System.out.println("check 1 : " + connector.check());
        System.out.println("getRunning Before stop 1 : " + connector.getRunningInstances());
        System.out.println("stop 1: " + connector.stop());
        System.out.println("getRunning After stop 1 : " + connector.getRunningInstances());
        System.out.println("check 1 : " + connector.check());
        System.out.println("listFile 1 : " + connector.listCanalLog());
        System.out.println("getFile 1 : " + connector.canalLog(10));
        connector.disconnect();

        connector.connect();
        System.out.println("check 2 : " + connector.check());
        System.out.println("get Running Before start : " + connector.getRunningInstances());
        System.out.println("start 2 : " + connector.start());
        System.out.println("get Running After start : " + connector.getRunningInstances());
        System.out.println("check 2 : " + connector.check());
        System.out.println("check after before example 2 : " + connector.checkInstance("example"));
        System.out.println("stop example 2 : " + connector.stopInstance("example"));
        System.out.println("check example 2 : " + connector.checkInstance("example"));
        System.out.println("start example 2 : " + connector.startInstance("example"));
        System.out.println("check after start example 2 : " + connector.checkInstance("example"));
        System.out.println("listFile 2 : " + connector.listCanalLog());
        System.out.println("getFile 2 : " + connector.canalLog(10));

        System.out.println("listFile 3 : " + connector.listInstanceLog("example"));
        System.out.println("getFile 3 : " + connector.instanceLog("example", "example.log", 10));
        connector.disconnect();
    }
}

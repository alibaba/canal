package com.alibaba.otter.canal.deployer.mbean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Canal Server Agent 用于远程JMX调用
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
public class CanalServerAgent implements NotificationListener {

    private static final Logger         logger      = LoggerFactory.getLogger(CanalServerBean.class);

    private MBeanServer                 mBeanServer = ManagementFactory.getPlatformMBeanServer();

    private String                      ip          = "0.0.0.0";

    private int                         port;

    private CanalServerMXBean           canalServerMbean;

    private volatile JMXConnectorServer cs;

    public CanalServerAgent(String ip, int port, CanalServerMXBean canalServerMbean){
        if (StringUtils.isNotEmpty(ip)) {
            this.ip = ip;
        }
        this.port = port;
        this.canalServerMbean = canalServerMbean;
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {

    }

    public synchronized void start() {
        try {
            if (cs == null) {
                ObjectName name = new ObjectName("CanalServerAgent:type=CanalServerStatus");
                mBeanServer.registerMBean(canalServerMbean, name);

                LocateRegistry.createRegistry(port);

                JMXServiceURL jmxServiceURL = new JMXServiceURL(
                    "service:jmx:rmi:///jndi/rmi://" + ip + ":" + port + "/jmxrmi");
                cs = JMXConnectorServerFactory.newJMXConnectorServer(jmxServiceURL, null, mBeanServer);
                cs.start();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public synchronized void stop() {
        if (cs != null) {
            try {
                cs.stop();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}

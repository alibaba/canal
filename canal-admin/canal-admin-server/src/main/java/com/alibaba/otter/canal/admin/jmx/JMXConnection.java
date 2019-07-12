package com.alibaba.otter.canal.admin.jmx;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.*;

public class JMXConnection {

    private Logger            logger = LoggerFactory.getLogger(JMXConnection.class);

    private String            ip;
    private Integer           port;
    private JMXConnector      jmxc;
    private CanalServerMXBean canalServerMXBean;

    public JMXConnection(String ip, Integer port){
        this.ip = ip;
        this.port = port;
    }

    public void connect() {
        try {
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + ip + ":" + port + "/jmxrmi");
            // jmxc = JMXConnectorFactory.connect(url, null);
            jmxc = connectWithTimeout(url, 3, TimeUnit.SECONDS);
            MBeanServerConnection mBeanServerConnection = jmxc.getMBeanServerConnection();
            ObjectName name = new ObjectName("CanalServerAgent:type=CanalServerStatus");
            mBeanServerConnection.addNotificationListener(name, (notification, handback) -> {
            }, null, null);
            canalServerMXBean = MBeanServerInvocationHandler
                .newProxyInstance(mBeanServerConnection, name, CanalServerMXBean.class, false);
        } catch (Exception e) {
            throw new ServiceException(e.getMessage(), e);
        }
    }

    private static JMXConnector connectWithTimeout(final JMXServiceURL url, long timeout,
                                                  TimeUnit unit) throws IOException {
        final BlockingQueue<Object> mailbox = new ArrayBlockingQueue<>(1);
        ExecutorService executor = Executors.newSingleThreadExecutor(daemonThreadFactory);
        executor.submit(() -> {
            try {
                JMXConnector connector = JMXConnectorFactory.connect(url);
                if (!mailbox.offer(connector)) connector.close();
            } catch (Throwable t) {
                mailbox.offer(t);
            }
        });
        Object result;
        try {
            result = mailbox.poll(timeout, unit);
            if (result == null) {
                if (!mailbox.offer("")) result = mailbox.take();
            }
        } catch (InterruptedException e) {
            throw initCause(new InterruptedIOException(e.getMessage()), e);
        } finally {
            executor.shutdown();
        }
        if (result == null) throw new SocketTimeoutException("Connect timed out: " + url);
        if (result instanceof JMXConnector) return (JMXConnector) result;
        try {
            throw (Throwable) result;
        } catch (IOException | RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            // In principle this can't happen but we wrap it anyway
            throw new IOException(e.toString(), e);
        }
    }

    private static <T extends Throwable> T initCause(T wrapper, Throwable wrapped) {
        wrapper.initCause(wrapped);
        return wrapper;
    }

    private static class DaemonThreadFactory implements ThreadFactory {

        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    }

    private static final ThreadFactory daemonThreadFactory = new DaemonThreadFactory();

    public CanalServerMXBean getCanalServerMXBean() {
        if (jmxc == null) {
            connect();
        }
        return canalServerMXBean;
    }

    public void close() {
        try {
            if (jmxc != null) {
                jmxc.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            jmxc = null;
        }
    }
}

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
import java.util.function.Function;

/**
 * Canal Server JMX 控制层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
public class JMXConnection {

    private static final Logger logger = LoggerFactory.getLogger(JMXConnection.class);

    private String              ip;
    private Integer             port;
    private JMXConnector        jmxc;
    private CanalServerMXBean   canalServerMXBean;

    public JMXConnection(String ip, Integer port){
        this.ip = ip;
        this.port = port;
    }

    /**
     * 执行相关操作
     *
     * @param ip jmx ip
     * @param port jmx port
     * @param function 执行方法
     * @param <R> 返回泛型
     * @return 执行结果
     */
    public static <R> R execute(String ip, int port, Function<CanalServerMXBean, R> function) {
        JMXConnection jmxConnection = new JMXConnection(ip, port);
        try {
            CanalServerMXBean canalServerMXBean = jmxConnection.getCanalServerMXBean();
            return function.apply(canalServerMXBean);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            jmxConnection.close();
        }
        return null;
    }

    /**
     * 连接远程jmx
     */
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

    /**
     * 带超时的jmx连接器
     *
     * @param url jmx url
     * @param timeout 超时时间
     * @param unit 超时时间单位
     * @return JMXConnector
     * @throws IOException 连接异常
     */
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

    /**
     * 获取MBean
     *
     * @return canalServerMXBean
     */
    public CanalServerMXBean getCanalServerMXBean() {
        if (jmxc == null) {
            connect();
        }
        return canalServerMXBean;
    }

    /**
     * 关闭jmx连接
     */
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

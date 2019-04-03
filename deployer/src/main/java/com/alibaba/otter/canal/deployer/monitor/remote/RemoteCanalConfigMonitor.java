package com.alibaba.otter.canal.deployer.monitor.remote;

import java.util.Properties;

/**
 * 远程canal.properties配置监听器接口
 *
 * @author rewerma 2019-01-25 下午05:20:16
 * @version 1.0.0
 */
public interface RemoteCanalConfigMonitor {
    void onChange(Properties properties);
}

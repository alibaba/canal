package com.alibaba.otter.canal.adapter.launcher.loader;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import com.alibaba.otter.canal.adapter.launcher.monitor.AdapterRemoteConfigMonitor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.adapter.launcher.common.SyncSwitch;
import com.alibaba.otter.canal.adapter.launcher.config.AdapterCanalConfig;
import com.alibaba.otter.canal.adapter.launcher.config.SpringContext;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

/**
 * 适配器启动业务类
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@Component
@RefreshScope
public class CanalAdapterService {

    private static final Logger        logger        = LoggerFactory.getLogger(CanalAdapterService.class);

    private CanalAdapterLoader         adapterLoader;

    @Resource
    private ContextRefresher           contextRefresher;

    @Resource
    private AdapterCanalConfig         adapterCanalConfig;
    @Resource
    private Environment                env;

    // 注入bean保证优先注册
    @Resource
    private SpringContext              springContext;
    @Resource
    private SyncSwitch                 syncSwitch;

    private volatile boolean           running       = false;

    private AdapterRemoteConfigMonitor configMonitor = null;

    @PostConstruct
    public synchronized void init() {
        if (running) {
            return;
        }
        try {
            // 加载远程配置
            String jdbcUrl = env.getProperty("canal.manager.jdbc.url");
            if (StringUtils.isNotEmpty(jdbcUrl)) {
                String jdbcUsername = env.getProperty("canal.manager.jdbc.username");
                String jdbcPassword = env.getProperty("canal.manager.jdbc.password");
                configMonitor = new AdapterRemoteConfigMonitor(jdbcUrl, jdbcUsername, jdbcPassword);
                configMonitor.loadRemoteConfig();
                configMonitor.loadRemoteAdapterConfigs();
                contextRefresher.refresh();
                configMonitor.start();
            }

            logger.info("## start the canal client adapters.");
            adapterLoader = new CanalAdapterLoader(adapterCanalConfig);
            adapterLoader.init();
            running = true;
            logger.info("## the canal client adapters are running now ......");
        } catch (Exception e) {
            logger.error("## something goes wrong when starting up the canal client adapters:", e);
        }
    }

    @PreDestroy
    public synchronized void destroy() {
        if (!running) {
            return;
        }
        try {
            running = false;
            logger.info("## stop the canal client adapters");
            if (configMonitor != null) {
                configMonitor.destroy();
            }

            if (adapterLoader != null) {
                adapterLoader.destroy();
                adapterLoader = null;
            }
            for (DruidDataSource druidDataSource : DatasourceConfig.DATA_SOURCES.values()) {
                try {
                    druidDataSource.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            DatasourceConfig.DATA_SOURCES.clear();
        } catch (Throwable e) {
            logger.warn("## something goes wrong when stopping canal client adapters:", e);
        } finally {
            logger.info("## canal client adapters are down.");
        }
    }
}

package com.alibaba.otter.canal.adapter.launcher.loader;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.adapter.launcher.config.AdapterCanalConfig;
import com.alibaba.otter.canal.adapter.launcher.config.AdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;

@Component
public class CanalAdapterService {

    private static final Logger       logger = LoggerFactory.getLogger(CanalAdapterService.class);

    private static CanalAdapterLoader adapterLoader;

    @Resource
    private AdapterCanalConfig        adapterCanalConfig;

    // 注入配置保证配置优先先注册
    @Resource
    private AdapterConfig             adapterConfig;

    @PostConstruct
    public void init() {
        if (adapterLoader == null) {
            try {
                logger.info("## start the canal client adapters.");
                adapterLoader = new CanalAdapterLoader(adapterCanalConfig);
                adapterLoader.init();
                logger.info("## the canal client adapters are running now ......");
            } catch (Throwable e) {
                logger.error("## something goes wrong when starting up the canal client adapters:", e);
                System.exit(0);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            logger.info("## stop the canal client adapters");
            if (adapterLoader != null) {
                adapterLoader.destroy();
            }
            for (DruidDataSource druidDataSource : DatasourceConfig.DATA_SOURCES.values()) {
                try {
                    druidDataSource.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (Throwable e) {
            logger.warn("## something goes wrong when stopping canal client adapters:", e);
        } finally {
            logger.info("## canal client adapters are down.");
        }
    }
}

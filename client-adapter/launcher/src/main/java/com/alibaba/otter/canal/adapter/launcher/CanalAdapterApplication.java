package com.alibaba.otter.canal.adapter.launcher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import com.alibaba.otter.canal.adapter.launcher.loader.CanalAdapterLoader;
import com.alibaba.otter.canal.client.adapter.support.CanalClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;


@SpringBootApplication
public class CanalAdapterApplication {
    private static final Logger logger = LoggerFactory.getLogger(CanalAdapterApplication.class);

    private static CanalAdapterLoader adapterLoader;

    public static void main(String[] args) {
        new SpringApplicationBuilder(CanalAdapterApplication.class).run(args);
    }

    @Resource
    private CanalClientConfig canalClientConf;

    @PostConstruct
    public void init() {
        if (adapterLoader == null) {
            try {
                logger.info("## start the canal client adapters.");
                adapterLoader = new CanalAdapterLoader(canalClientConf);
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
        } catch (Throwable e) {
            logger.warn("## something goes wrong when stopping canal client adapters:", e);
        } finally {
            logger.info("## canal client adapters are down.");
        }
    }
}

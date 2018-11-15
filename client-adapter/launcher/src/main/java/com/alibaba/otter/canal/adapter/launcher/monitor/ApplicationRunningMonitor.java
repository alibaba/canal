package com.alibaba.otter.canal.adapter.launcher.monitor;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.adapter.launcher.loader.CanalAdapterService;
import com.alibaba.otter.canal.client.adapter.support.Constant;

@Component
public class ApplicationRunningMonitor {

    private static final Logger      logger                = LoggerFactory.getLogger(ApplicationRunningMonitor.class);

    @Resource
    private ContextRefresher         contextRefresher;

    @Resource
    private CanalAdapterService      canalAdapterService;

    private ScheduledExecutorService scheduled             = Executors.newScheduledThreadPool(1);

    private static volatile long     applicationLastModify = -1L;

    @PostConstruct
    public void init() {
        URL url = this.getClass().getClassLoader().getResource("");
        String path;
        if (url != null) {
            path = url.getPath();
        } else {
            path = new File("").getAbsolutePath();
        }
        File file = null;
        if (path != null) {
            file = new File(path + ".." + File.separator + Constant.CONF_DIR + File.separator + "application.yml");
            if (!file.exists()) {
                file = new File(path + "application.yml");
            }
        }
        if (file == null || !file.exists()) {
            throw new RuntimeException("application.yml config file not found.");
        }
        File configFile = file;
        scheduled.scheduleWithFixedDelay(() -> {
            if (applicationLastModify == -1L) {
                applicationLastModify = configFile.lastModified();
            } else {
                if (configFile.lastModified() != applicationLastModify) {
                    applicationLastModify = configFile.lastModified();

                    try {
                        // 检查yml格式
                        new Yaml().loadAs(new FileReader(configFile), Map.class);

                        canalAdapterService.destroy();

                        // refresh context
                        contextRefresher.refresh();

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                        canalAdapterService.init();
                        logger.info("## adapter application config reloaded.");
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void destroy() {
        scheduled.shutdown();
    }
}

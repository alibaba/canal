package com.alibaba.otter.canal.adapter.launcher.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.otter.canal.adapter.launcher.loader.CanalAdapterService;
import com.alibaba.otter.canal.client.adapter.support.Util;

@Component
public class ApplicationConfigMonitor {

    private static final Logger   logger = LoggerFactory.getLogger(ApplicationConfigMonitor.class);

    @Resource
    private ContextRefresher      contextRefresher;

    @Resource
    private CanalAdapterService   canalAdapterService;

    private FileAlterationMonitor fileMonitor;

    @PostConstruct
    public void init() {
        File confDir = Util.getConfDirPath();
        try {
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                FileFilterUtils.and(FileFilterUtils.fileFileFilter(),
                    FileFilterUtils.prefixFileFilter("application"),
                    FileFilterUtils.suffixFileFilter("yml")));
            FileListener listener = new FileListener();
            observer.addListener(listener);
            fileMonitor = new FileAlterationMonitor(3000, observer);
            fileMonitor.start();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);
            try {
                // 检查yml格式
                new Yaml().loadAs(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8), Map.class);

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
}

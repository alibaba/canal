package com.alibaba.otter.canal.admin.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.service.CanalConfigService;

/**
 * Canal配置信息业务层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@Service
public class CanalConfigServiceImpl implements CanalConfigService {

    private Logger              logger               = LoggerFactory.getLogger(CanalConfigServiceImpl.class);

    private static final String CANAL_GLOBAL_CONFIG  = "canal.properties";
    private static final String CANAL_ADAPTER_CONFIG = "application.yml";

    public CanalConfig getCanalConfig() {
        long id = 1L;
        CanalConfig config = CanalConfig.find.byId(id);
        if (config == null) {
            String context = loadDefaultConf(CANAL_GLOBAL_CONFIG);
            if (context == null) {
                return null;
            }

            config = new CanalConfig();
            config.setId(id);
            config.setName(CANAL_GLOBAL_CONFIG);
            config.setModifiedTime(new Date());
            config.setContent(context);
            return config;
        }

        return config;
    }

    public CanalConfig getCanalConfigSummary() {
        return CanalConfig.find.query()
            .setDisableLazyLoading(true)
            .select("name, modifiedTime")
            .where()
            .eq("id", 1L)
            .findOne();
    }

    public CanalConfig getAdapterConfig() {
        long id = 2L;
        CanalConfig config = CanalConfig.find.byId(id);
        if (config == null) {
            String context = loadDefaultConf(CANAL_ADAPTER_CONFIG);
            if (context == null) {
                return null;
            }

            config = new CanalConfig();
            config.setId(id);
            config.setName(CANAL_ADAPTER_CONFIG);
            config.setModifiedTime(new Date());
            config.setContent(context);
            return config;
        }

        return config;
    }

    public void updateContent(CanalConfig canalConfig) {
        try {
            canalConfig.insert();
        } catch (Throwable e) {
            canalConfig.update();
        }
    }

    private String loadDefaultConf(String confFileName) {
        InputStream input = null;
        try {
            input = Thread.currentThread().getContextClassLoader().getResourceAsStream("conf/" + confFileName);
            if (input == null) {
                return null;
            }

            return StringUtils.join(IOUtils.readLines(input), "\n");
        } catch (IOException e) {
            logger.error("find " + confFileName + " is error!", e);
            return null;
        } finally {
            if (input != null) {
                IOUtils.closeQuietly(input);
            }
        }
    }
}

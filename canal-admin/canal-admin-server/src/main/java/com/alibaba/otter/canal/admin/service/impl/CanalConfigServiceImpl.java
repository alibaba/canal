package com.alibaba.otter.canal.admin.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.admin.common.exception.ServiceException;
import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.CanalConfigService;
import com.alibaba.otter.canal.protocol.SecurityUtil;

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

    public CanalConfig getCanalConfig(Long clusterId, Long serverId) {
        CanalConfig config = null;
        if (clusterId != null && clusterId != 0) {
            config = CanalConfig.find.query().where().eq("clusterId", clusterId).findOne();
        } else if (serverId != null && serverId != 0) {
            config = CanalConfig.find.query().where().eq("serverId", serverId).findOne();
            if (config == null) {
                NodeServer nodeServer = NodeServer.find.byId(serverId);
                if (nodeServer != null) {
                    Long cid = nodeServer.getClusterId();
                    if (cid != null) {
                        config = CanalConfig.find.query().where().eq("clusterId", cid).findOne();
                    }
                }
            }
        } else {
            throw new ServiceException("clusterId and serverId are all empty");
        }
        if (config == null) {
            config = new CanalConfig();
            config.setName(CANAL_GLOBAL_CONFIG);
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
            String contentMd5 = SecurityUtil.md5String(canalConfig.getContent());
            canalConfig.setContentMd5(contentMd5);
        } catch (NoSuchAlgorithmException e) {
            // ignore
        }
        if (canalConfig.getId() != null) {
            CanalConfig canalConfigTmp = CanalConfig.find.byId(canalConfig.getId());
            if (canalConfigTmp != null && canalConfigTmp.getClusterId() != null) {
                canalConfig.setServerId(null);
            }
            canalConfig.update("serverId", "content", "contentMd5");
        } else {
            canalConfig.save();
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

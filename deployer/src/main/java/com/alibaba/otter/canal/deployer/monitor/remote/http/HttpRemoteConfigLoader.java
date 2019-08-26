package com.alibaba.otter.canal.deployer.monitor.remote.http;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.otter.canal.common.utils.CommonUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.deployer.CanalConstants;
import com.alibaba.otter.canal.deployer.monitor.remote.*;
import com.google.common.collect.MapMaker;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpRemoteConfigLoader implements RemoteConfigLoader {

    private final static Logger logger = LoggerFactory.getLogger(HttpRemoteConfigLoader.class);

    private String baseUrl;
    private String username;
    private String password;

    private String token;

    private HttpHelper httpHelper;

    private long currentConfigTimestamp = 0;
    private Map<String, ConfigItem> remoteInstanceConfigs = new MapMaker().makeMap();

    private RemoteInstanceMonitor remoteInstanceMonitor = new RemoteInstanceMonitorImpl();

    private long scanIntervalInSecond = 5;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2,
            new NamedThreadFactory("remote-http-canal-config-scan"));

    public HttpRemoteConfigLoader(String baseUrl, String username, String password) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        httpHelper = new HttpHelper();
    }

    private String login4Token(HttpHelper httpHelper) {
        Map<String, Object> reqBody = new HashMap<>();
        reqBody.put("username", username);
        reqBody.put("password", password);
        String response = httpHelper.post(baseUrl + "/api/v1/user/login", null, reqBody, 5000);
        ResponseModel<JSONObject> resp = JSONObject.parseObject(response, new TypeReference<ResponseModel<JSONObject>>() {
        });
        if (resp.getCode() != 20000) {
            throw new RuntimeException("requestPost for login error: " + resp.getMessage());
        }
        return (String) resp.getData().get("token");
    }

    @Override
    public synchronized Properties loadRemoteConfig() {
        Properties properties = null;
        try {
            ConfigItem configItem = getRemoteCanalConfig();
            if (configItem != null) {
                if (configItem.getModifiedTime() != currentConfigTimestamp) { // 修改时就不同说明配置有更新
                    Map<String, String> heads = new HashMap<>();
                    heads.put("X-Token", token);
                    String response = httpHelper
                            .get(baseUrl + "/api/v1/canal/config", heads, 5000);
                    ResponseModel<ConfigItem> resp = JSONObject.parseObject(response, new TypeReference<ResponseModel<ConfigItem>>() {
                    });
                    currentConfigTimestamp = configItem.getModifiedTime();
                    overrideLocalCanalConfig(resp.getData().getContent());
                    properties = new Properties();
                    properties
                            .load(new ByteArrayInputStream(resp.getData().getContent().getBytes(StandardCharsets.UTF_8)));
                    scanIntervalInSecond = Integer
                            .parseInt(properties.getProperty(CanalConstants.CANAL_AUTO_SCAN_INTERVAL, "5"));
                    logger.info("## Loaded http remote canal config: canal.properties ");
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
        return properties;
    }

    /**
     * 获取远程canal server配置摘要(无配置内容)
     *
     * @return 配置摘要
     */
    private ConfigItem getRemoteCanalConfig() {
        if (StringUtils.isEmpty(token)) {
            token = login4Token(httpHelper);
        }

        Map<String, String> heads = new HashMap<>();
        heads.put("X-Token", token);
        String response = httpHelper.get(baseUrl + "/api/v1/canal/config/summary", heads, 5000);
        ResponseModel<ConfigItem> resp = JSONObject.parseObject(response, new TypeReference<ResponseModel<ConfigItem>>() {
        });
        if (resp.getCode() == 50014) {
            // token 失效
            token = login4Token(httpHelper);
            heads.put("X-Token", token);
            response = httpHelper.get(baseUrl + "/api/v1/canal/config/summary", heads, 5000);
            resp = JSONObject.parseObject(response, new TypeReference<ResponseModel<ConfigItem>>() {
            });
        }
        if (resp.getCode() != 20000) {
            throw new RuntimeException("requestGet for canal config error: " + resp.getMessage());
        }
        return resp.getData();
    }

    @Override
    public synchronized void loadRemoteInstanceConfigs() {
        try {
            // 加载远程instance配置
            loadModifiedInstanceConfigs();
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    /**
     * 加载远程instance新增、修改、删除配置
     */
    private void loadModifiedInstanceConfigs() {
        if (StringUtils.isEmpty(token)) {
            token = login4Token(httpHelper);
        }

        Map<String, ConfigItem> remoteConfigStatus = new HashMap<>();

        Map<String, String> heads = new HashMap<>();
        heads.put("X-Token", token);
        String response = httpHelper
                .get(baseUrl + "/api/v1/canal/instances", heads, 5000);
        ResponseModel<List<ConfigItem>> resp = JSONObject.parseObject(response,
                new TypeReference<ResponseModel<List<ConfigItem>>>() {
                });
        if (resp.getCode() == 50014) {
            // token 失效
            token = login4Token(httpHelper);
            heads.put("X-Token", token);
            response = httpHelper.get(baseUrl + "/api/v1/canal/instances", heads, 5000);
            resp = JSONObject.parseObject(response, new TypeReference<ResponseModel<List<ConfigItem>>>() {
            });
        }
        if (resp.getCode() != 20000) {
            throw new RuntimeException("requestGet for canal instances error: " + resp.getMessage());
        }
        for (ConfigItem configItem : resp.getData()) {
            remoteConfigStatus.put(configItem.getName(), configItem);
        }

        if (!remoteConfigStatus.isEmpty()) {
            List<Long> changedIds = new ArrayList<>();

            for (ConfigItem remoteConfigStat : remoteConfigStatus.values()) {
                ConfigItem currentConfig = remoteInstanceConfigs.get(remoteConfigStat.getName());
                if (currentConfig == null) {
                    // 新增
                    changedIds.add(remoteConfigStat.getId());
                } else {
                    // 修改
                    if (currentConfig.getModifiedTime() != remoteConfigStat.getModifiedTime()) {
                        changedIds.add(remoteConfigStat.getId());
                    }
                }
            }
            if (!changedIds.isEmpty()) {
                for (Long changedId : changedIds) {
                    String response2 = httpHelper
                            .get(baseUrl + "/api/v1/canal/instance/" + changedId, heads, 5000);
                    ResponseModel<ConfigItem> resp2 = JSONObject.parseObject(response2,
                            new TypeReference<ResponseModel<ConfigItem>>() {
                            });
                    ConfigItem configItemNew = resp2.getData();
                    remoteInstanceConfigs.put(configItemNew.getName(), configItemNew);
                    remoteInstanceMonitor.onModify(configItemNew);
                }
            }
        }

        for (String name : remoteInstanceConfigs.keySet()) {
            if (!remoteConfigStatus.containsKey(name)) {
                // 删除
                remoteInstanceConfigs.remove(name);
                remoteInstanceMonitor.onDelete(name);
            }
        }
    }

    @Override
    public synchronized void startMonitor(RemoteCanalConfigMonitor remoteCanalConfigMonitor) {

        executor.scheduleWithFixedDelay(new Runnable() {

            public void run() {
                // 监听canal.properties变化
                try {
                    Properties properties = loadRemoteConfig();
                    if (properties != null) {
                        remoteCanalConfigMonitor.onChange(properties);
                    }
                } catch (Throwable e) {
                    logger.error("Scan remote canal config failed", e);
                }

                // 监听instance变化
                try {
                    loadRemoteInstanceConfigs();
                } catch (Throwable e) {
                    logger.error("Scan remote instance config failed", e);
                }
            }

        }, 10, scanIntervalInSecond, TimeUnit.SECONDS);
    }

    /**
     * 覆盖本地 canal.properties
     *
     * @param content 远程配置内容文本
     */
    private void overrideLocalCanalConfig(String content) {
        try (FileWriter writer = new FileWriter(CommonUtils.getConfPath() + "canal.properties")) {
            writer.write(content);
            writer.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public synchronized void destroy() {
        if (httpHelper != null) {
            httpHelper.close();
        }
    }
}

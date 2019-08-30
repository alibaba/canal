package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;

public interface PollingConfigServer {

    CanalConfig getChangedConfig(String ip, Integer port, String md5);

    CanalInstanceConfig getInstancesConfig(String ip, Integer port, String md5);

    CanalInstanceConfig getInstanceConfig(String destination, String md5);
}

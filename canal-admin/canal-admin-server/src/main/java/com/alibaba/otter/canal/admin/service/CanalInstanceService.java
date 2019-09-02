package com.alibaba.otter.canal.admin.service;

import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.model.Pager;

/**
 * Canal实例配置信息业务层接口
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
public interface CanalInstanceService {

    Pager<CanalInstanceConfig> findList(CanalInstanceConfig canalInstanceConfig, Pager<CanalInstanceConfig> pager);

    void save(CanalInstanceConfig canalInstanceConfig);

    CanalInstanceConfig detail(Long id);

    void updateContent(CanalInstanceConfig canalInstanceConfig);

    void delete(Long id);

    Map<String, String> remoteInstanceLog(Long id, Long nodeId);

    boolean remoteOperation(Long id, Long nodeId, String option);

    boolean instanceOperation(Long id, String option);

    List<CanalInstanceConfig> findActiveInstanceByServerId(Long serverId);
}

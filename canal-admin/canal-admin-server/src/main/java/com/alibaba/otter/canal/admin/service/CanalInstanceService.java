package com.alibaba.otter.canal.admin.service;

import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;

import java.util.List;
import java.util.Map;

/**
 * Canal实例配置信息业务层接口
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
public interface CanalInstanceService {

    List<CanalInstanceConfig> findList(CanalInstanceConfig canalInstanceConfig);

    void save(CanalInstanceConfig canalInstanceConfig);

    CanalInstanceConfig detail(Long id);

    void updateContent(CanalInstanceConfig canalInstanceConfig);

    void delete(Long id);

    Map<String, String> remoteInstanceLog(Long id, Long nodeId);

    boolean remoteOperation(Long id, Long nodeId, String option);
}

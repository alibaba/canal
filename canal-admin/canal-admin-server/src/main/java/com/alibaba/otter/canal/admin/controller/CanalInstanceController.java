package com.alibaba.otter.canal.admin.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.service.CanalInstanceService;

/**
 * Canal Instance配置管理控制层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/{env}/canal")
public class CanalInstanceController {

    @Autowired
    CanalInstanceService canalInstanceConfigService;

    /**
     * 实例配置列表
     *
     * @param canalInstanceConfig 查询对象
     * @param env 环境变量
     * @return 实例列表
     */
    @GetMapping(value = "/instances")
    public BaseModel<List<CanalInstanceConfig>> nodeServers(CanalInstanceConfig canalInstanceConfig,
                                                            @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.findList(canalInstanceConfig));
    }

    /**
     * 保存实例配置
     *
     * @param canalInstanceConfig 实例配置对象
     * @param env 环境变量
     * @return 是否成功
     */
    @PostMapping(value = "/instance")
    public BaseModel<String> save(@RequestBody CanalInstanceConfig canalInstanceConfig, @PathVariable String env) {
        canalInstanceConfigService.save(canalInstanceConfig);
        return BaseModel.getInstance("success");
    }

    /**
     * 实例详情信息
     *
     * @param id 实例配置id
     * @param env 环境变量
     * @return 实例信息
     */
    @GetMapping(value = "/instance/{id}")
    public BaseModel<CanalInstanceConfig> detail(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.detail(id));
    }

    /**
     * 修改实例配置
     *
     * @param canalInstanceConfig 实例配置信息
     * @param env 环境变量
     * @return 是否成功
     */
    @PutMapping(value = "/instance")
    public BaseModel<String> update(@RequestBody CanalInstanceConfig canalInstanceConfig, @PathVariable String env) {
        canalInstanceConfigService.updateContent(canalInstanceConfig);
        return BaseModel.getInstance("success");
    }

    /**
     * 删除实例配置
     *
     * @param id 实例配置id
     * @param env 环境变量
     * @return 是否成功
     */
    @DeleteMapping(value = "/instance/{id}")
    public BaseModel<String> delete(@PathVariable Long id, @PathVariable String env) {
        canalInstanceConfigService.delete(id);
        return BaseModel.getInstance("success");
    }

    /**
     * 启动远程实例
     *
     * @param id 实例配置id
     * @param env 环境变量
     * @return 是否成功
     */
    @PutMapping(value = "/instance/start/{id}/{nodeId}")
    public BaseModel<Boolean> start(@PathVariable Long id, @PathVariable Long nodeId, @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.remoteOperation(id, nodeId, "start"));
    }

    /**
     * 关闭远程实例
     *
     * @param id 实例配置id
     * @param nodeId 节点id
     * @param env 环境变量
     * @return 是否成功
     */
    @PutMapping(value = "/instance/stop/{id}/{nodeId}")
    public BaseModel<Boolean> stop(@PathVariable Long id, @PathVariable Long nodeId, @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.remoteOperation(id, nodeId, "stop"));
    }

    /**
     * 获取远程实例运行日志
     *
     * @param id 实例配置id
     * @param nodeId 节点id
     * @param env 环境变量
     * @return 实例日志信息
     */
    @GetMapping(value = "/instance/log/{id}/{nodeId}")
    public BaseModel<Map<String, String>> instanceLog(@PathVariable Long id, @PathVariable Long nodeId,
                                                      @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.remoteInstanceLog(id, nodeId));
    }
}

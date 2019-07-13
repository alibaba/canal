package com.alibaba.otter.canal.admin.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.CanalInstanceConfig;
import com.alibaba.otter.canal.admin.service.CanalInstanceService;

@RestController
@RequestMapping("/api/{env}/canal")
public class CanalInstanceController {

    @Autowired
    CanalInstanceService canalInstanceConfigService;

    @GetMapping(value = "/instances")
    public BaseModel<List<CanalInstanceConfig>> nodeServers(CanalInstanceConfig canalInstanceConfig,
                                                            @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.findList(canalInstanceConfig));
    }

    @PostMapping(value = "/instance")
    public BaseModel<String> save(@RequestBody CanalInstanceConfig canalInstanceConfig, @PathVariable String env) {
        canalInstanceConfigService.save(canalInstanceConfig);
        return BaseModel.getInstance("success");
    }

    @GetMapping(value = "/instance/{id}")
    public BaseModel<CanalInstanceConfig> detail(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.detail(id));
    }

    @PutMapping(value = "/instance")
    public BaseModel<String> update(@RequestBody CanalInstanceConfig canalInstanceConfig, @PathVariable String env) {
        canalInstanceConfigService.updateContent(canalInstanceConfig);
        return BaseModel.getInstance("success");
    }

    @DeleteMapping(value = "/instance/{id}")
    public BaseModel<String> delete(@PathVariable Long id, @PathVariable String env) {
        canalInstanceConfigService.delete(id);
        return BaseModel.getInstance("success");
    }

    @PutMapping(value = "/instance/start/{id}")
    public BaseModel<Boolean> start(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.remoteOperation(id, null, "start"));
    }

    @PutMapping(value = "/instance/stop/{id}/{nodeId}")
    public BaseModel<Boolean> stop(@PathVariable Long id, @PathVariable Long nodeId, @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.remoteOperation(id, nodeId, "stop"));
    }
}

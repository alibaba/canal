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

    @GetMapping(value = "/canalInstances")
    public BaseModel<List<CanalInstanceConfig>> nodeServers(CanalInstanceConfig canalInstanceConfig,
                                                            @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.findList(canalInstanceConfig));
    }

    @PostMapping(value = "/canalInstance")
    public BaseModel<String> save(@RequestBody CanalInstanceConfig canalInstanceConfig, @PathVariable String env) {
        canalInstanceConfigService.save(canalInstanceConfig);
        return BaseModel.getInstance("success");
    }

    @GetMapping(value = "/canalInstance/{id}")
    public BaseModel<CanalInstanceConfig> detail(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(canalInstanceConfigService.detail(id));
    }

    @PutMapping(value = "/canalInstance")
    public BaseModel<String> update(@RequestBody  CanalInstanceConfig canalInstanceConfig, @PathVariable String env) {
        canalInstanceConfigService.updateContent(canalInstanceConfig);
        return BaseModel.getInstance("success");
    }

    @DeleteMapping(value = "/nodeServer/{id}")
    public BaseModel<String> delete(@PathVariable Long id, @PathVariable String env) {
        canalInstanceConfigService.delete(id);
        return BaseModel.getInstance("success");
    }
}

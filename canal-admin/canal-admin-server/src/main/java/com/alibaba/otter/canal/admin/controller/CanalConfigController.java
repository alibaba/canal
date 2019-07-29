package com.alibaba.otter.canal.admin.controller;

import com.alibaba.otter.canal.admin.model.BaseModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.service.CanalConfigService;

/**
 * Canal主配置管理控制层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/{env}/canal")
public class CanalConfigController {

    @Autowired
    CanalConfigService canalConfigService;

    /**
     * 获取配置信息
     *
     * @param env 环境变量
     * @return 配置信息
     */
    @GetMapping(value = "/config")
    public BaseModel<CanalConfig> canalConfig(@PathVariable String env) {
        return BaseModel.getInstance(canalConfigService.getCanalConfig());
    }

    /**
     * 修改配置
     *
     * @param canalConfig 配置信息对象
     * @param env 环境变量
     * @return 是否成功
     */
    @PutMapping(value = "/config")
    public BaseModel<String> updateConfig(@RequestBody CanalConfig canalConfig, @PathVariable String env) {
        canalConfigService.updateContent(canalConfig);
        return BaseModel.getInstance("success");
    }
}

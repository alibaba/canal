package com.alibaba.otter.canal.admin.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.otter.canal.admin.model.CanalConfig;
import com.alibaba.otter.canal.admin.service.CanalConfigService;

@RestController
@RequestMapping("/api/{env}/config")
public class CanalConfigController {

    @Autowired
    CanalConfigService canalConfigService;

    @RequestMapping(value = "/canal", method = RequestMethod.GET)
    public CanalConfig canalConfig(@PathVariable String env) {
        return canalConfigService.getCanalConfig();
    }
}

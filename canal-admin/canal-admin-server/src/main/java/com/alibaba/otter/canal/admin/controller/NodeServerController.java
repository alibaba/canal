package com.alibaba.otter.canal.admin.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.NodeServerService;

@RestController
@RequestMapping("/api/{env}")
public class NodeServerController {

    @Autowired
    NodeServerService nodeServerService;

    @GetMapping(value = "/nodeServers")
    public BaseModel<List<NodeServer>> nodeServers(NodeServer nodeServer, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.findList(nodeServer));
    }

    @PostMapping(value = "/nodeServer")
    public BaseModel<String> save(@RequestBody NodeServer nodeServer, @PathVariable String env) {
        nodeServerService.save(nodeServer);
        return BaseModel.getInstance("success");
    }

    @GetMapping(value = "/nodeServer/{id}")
    public BaseModel<NodeServer> detail(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.detail(id));
    }

    @PutMapping(value = "/nodeServer")
    public BaseModel<String> update(@RequestBody NodeServer nodeServer, @PathVariable String env) {
        nodeServerService.update(nodeServer);
        return BaseModel.getInstance("success");
    }

    @DeleteMapping(value = "/nodeServer/{id}")
    public BaseModel<String> delete(@PathVariable Long id, @PathVariable String env) {
        nodeServerService.delete(id);
        return BaseModel.getInstance("success");
    }

    @GetMapping(value = "/nodeServer/status")
    public BaseModel<Integer> status(@RequestParam String ip, @RequestParam Integer port, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteNodeStatus(ip, port));
    }

    @PutMapping(value = "/nodeServer/start/{id}")
    public BaseModel<Boolean> start(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteOperation(id, "start"));
    }

    @GetMapping(value = "/nodeServer/log/{id}")
    public BaseModel<String> log(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteCanalLog(id));
    }

    @PutMapping(value = "/nodeServer/stop/{id}")
    public BaseModel<Boolean> stop(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteOperation(id, "stop"));
    }
}

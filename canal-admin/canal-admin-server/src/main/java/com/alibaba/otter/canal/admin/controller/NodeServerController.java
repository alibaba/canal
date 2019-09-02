package com.alibaba.otter.canal.admin.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.model.Pager;
import com.alibaba.otter.canal.admin.service.NodeServerService;

/**
 * 节点信息控制层
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/{env}")
public class NodeServerController {

    @Autowired
    NodeServerService nodeServerService;

    /**
     * 获取所有节点信息列表
     *
     * @param nodeServer 筛选条件
     * @param env 环境变量
     * @return 节点信息列表
     */
    @GetMapping(value = "/nodeServers")
    public BaseModel<Pager<NodeServer>> nodeServers(NodeServer nodeServer, Pager<NodeServer> pager,
                                                    @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.findList(nodeServer, pager));
    }

    /**
     * 保存节点信息
     *
     * @param nodeServer 节点信息
     * @param env 环境变量
     * @return 是否成功
     */
    @PostMapping(value = "/nodeServer")
    public BaseModel<String> save(@RequestBody NodeServer nodeServer, @PathVariable String env) {
        nodeServerService.save(nodeServer);
        return BaseModel.getInstance("success");
    }

    /**
     * 获取节点信息详情
     *
     * @param id 节点信息id
     * @param env 环境变量
     * @return 检点信息
     */
    @GetMapping(value = "/nodeServer/{id}")
    public BaseModel<NodeServer> detail(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.detail(id));
    }

    /**
     * 修改节点信息
     *
     * @param nodeServer 节点信息
     * @param env 环境变量
     * @return 是否成功
     */
    @PutMapping(value = "/nodeServer")
    public BaseModel<String> update(@RequestBody NodeServer nodeServer, @PathVariable String env) {
        nodeServerService.update(nodeServer);
        return BaseModel.getInstance("success");
    }

    /**
     * 删除节点信息
     *
     * @param id 节点信息id
     * @param env 环境变量
     * @return 是否成功
     */
    @DeleteMapping(value = "/nodeServer/{id}")
    public BaseModel<String> delete(@PathVariable Long id, @PathVariable String env) {
        nodeServerService.delete(id);
        return BaseModel.getInstance("success");
    }

    /**
     * 获取远程节点运行状态
     *
     * @param ip 节点ip
     * @param port 节点端口
     * @param env 环境变量
     * @return 状态信息
     */
    @GetMapping(value = "/nodeServer/status")
    public BaseModel<Integer> status(@RequestParam String ip, @RequestParam Integer port, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteNodeStatus(ip, port));
    }

    /**
     * 启动远程节点
     *
     * @param id 节点id
     * @param env 环境变量
     * @return 是否成功
     */
    @PutMapping(value = "/nodeServer/start/{id}")
    public BaseModel<Boolean> start(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteOperation(id, "start"));
    }

    /**
     * 获取远程节点日志
     *
     * @param id 节点id
     * @param env 环境变量
     * @return 节点日志
     */
    @GetMapping(value = "/nodeServer/log/{id}")
    public BaseModel<String> log(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteCanalLog(id));
    }

    /**
     * 关闭远程节点
     *
     * @param id 节点id
     * @param env 环境变量
     * @return 是否成功
     */
    @PutMapping(value = "/nodeServer/stop/{id}")
    public BaseModel<Boolean> stop(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(nodeServerService.remoteOperation(id, "stop"));
    }
}

package com.alibaba.otter.canal.admin.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.admin.model.BaseModel;
import com.alibaba.otter.canal.admin.model.CanalCluster;
import com.alibaba.otter.canal.admin.model.NodeServer;
import com.alibaba.otter.canal.admin.service.CanalClusterService;
import com.alibaba.otter.canal.admin.service.NodeServerService;

@RestController
@RequestMapping("/api/{env}/canal")
public class CanalClusterController {

    @Autowired
    CanalClusterService canalClusterServic;

    @Autowired
    NodeServerService   nodeServerService;

    @GetMapping(value = "/clusters")
    public BaseModel<List<CanalCluster>> clusters(CanalCluster canalCluster, @PathVariable String env) {
        return BaseModel.getInstance(canalClusterServic.findList(canalCluster));
    }

    @PostMapping(value = "/cluster")
    public BaseModel<String> save(@RequestBody CanalCluster canalCluster, @PathVariable String env) {
        canalClusterServic.save(canalCluster);
        return BaseModel.getInstance("success");
    }

    @GetMapping(value = "/cluster/{id}")
    public BaseModel<CanalCluster> detail(@PathVariable Long id, @PathVariable String env) {
        return BaseModel.getInstance(canalClusterServic.detail(id));
    }

    @PutMapping(value = "/cluster")
    public BaseModel<String> update(@RequestBody CanalCluster canalCluster, @PathVariable String env) {
        canalClusterServic.update(canalCluster);
        return BaseModel.getInstance("success");
    }

    @DeleteMapping(value = "/cluster/{id}")
    public BaseModel<String> delete(@PathVariable Long id, @PathVariable String env) {
        canalClusterServic.delete(id);
        return BaseModel.getInstance("success");
    }

    @GetMapping(value = "/clustersAndServers")
    public BaseModel<List<?>> clustersAndServers(@PathVariable String env) {
        List<CanalCluster> clusters = canalClusterServic.findList(new CanalCluster());
        JSONObject group = new JSONObject();
        group.put("label", "集群");
        JSONArray jsonArray = new JSONArray();
        clusters.forEach(cluster -> {
            JSONObject item = new JSONObject();
            item.put("label", cluster.getName());
            item.put("value", "cluster:" + cluster.getId());
            jsonArray.add(item);
        });
        group.put("options", jsonArray);

        NodeServer param = new NodeServer();
        param.setClusterId(-1L);
        List<NodeServer> servers = nodeServerService.findAll(param); // 取所有standalone的节点
        JSONObject group2 = new JSONObject();
        group2.put("label", "单机主机");
        JSONArray jsonArray2 = new JSONArray();
        servers.forEach(server -> {
            JSONObject item = new JSONObject();
            item.put("label", server.getName());
            item.put("value", "server:" + server.getId());
            jsonArray2.add(item);
        });
        group2.put("options", jsonArray2);

        List<JSONObject> result = new ArrayList<>();
        result.add(group);
        result.add(group2);
        return BaseModel.getInstance(result);
    }

}

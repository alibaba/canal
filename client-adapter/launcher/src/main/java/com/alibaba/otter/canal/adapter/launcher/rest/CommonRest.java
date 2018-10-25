package com.alibaba.otter.canal.adapter.launcher.rest;

import java.util.*;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.adapter.launcher.common.SyncSwitch;
import com.alibaba.otter.canal.adapter.launcher.config.AdapterCanalConfig;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;
import com.alibaba.otter.canal.client.adapter.support.Result;

@RestController
public class CommonRest {

    private static Logger                 logger = LoggerFactory.getLogger(CommonRest.class);

    private ExtensionLoader<OuterAdapter> loader;

    @Resource
    private SyncSwitch                    syncSwitch;

    @PostConstruct
    public void init() {
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);
    }

    /**
     * Demo: curl http://127.0.0.1:8081/etl/hbase/mytest_person2.yml -X POST -d
     * "params=0,1,2"
     */
    @PostMapping("/etl/{type}/{task}")
    public EtlResult etl(@PathVariable String type, @PathVariable String task,
                         @RequestParam(name = "params", required = false) String params) {
        OuterAdapter adapter = loader.getExtension(type);
        List<String> paramArr = null;
        if (params != null) {
            String[] parmaArray = params.trim().split(";");
            paramArr = Arrays.asList(parmaArray);
        }

        return adapter.etl(task, paramArr);
    }

    /**
     * Demo: curl http://127.0.0.1:8081/count/hbase/mytest_person2.yml
     */
    @GetMapping("/count/{type}/{task}")
    public Map<String, Object> count(@PathVariable String type, @PathVariable String task) {
        OuterAdapter adapter = loader.getExtension(type);
        return adapter.count(task);
    }

    @GetMapping("/destinations")
    public List<Map<String, String>> destinations() {
        List<Map<String, String>> result = new ArrayList<>();
        Set<String> destinations = AdapterCanalConfig.DESTINATIONS;
        for (String destination : destinations) {
            Map<String, String> resMap = new LinkedHashMap<>();
            Boolean status = syncSwitch.status(destination);
            String resStatus = "none";
            if (status != null && status) {
                resStatus = "on";
            } else if (status != null && !status) {
                resStatus = "off";
            }
            resMap.put("destination", destination);
            resMap.put("status", resStatus);
            result.add(resMap);
        }
        return result;
    }

    @PutMapping("/syncSwitch/{destination}/{status}")
    public Result etl(@PathVariable String destination, @PathVariable String status) {
        if (status.equals("on")) {
            syncSwitch.on(destination);
            logger.info("#Destination: {} sync on", destination);
            return Result.createSuccess("实例: " + destination + " 开启同步成功");
        } else if (status.equals("off")) {
            syncSwitch.off(destination);
            logger.info("#Destination: {} sync off", destination);
            return Result.createSuccess("实例: " + destination + " 关闭同步成功");
        } else {
            Result result = new Result();
            result.setCode(50000);
            result.setMessage("实例: " + destination + " 操作失败");
            return result;
        }
    }

    @GetMapping("/syncSwitch/{destination}")
    public Map<String, String> etl(@PathVariable String destination) {
        Boolean status = syncSwitch.status(destination);
        String resStatus = "none";
        if (status != null && status) {
            resStatus = "on";
        } else if (status != null && !status) {
            resStatus = "off";
        }
        Map<String, String> res = new LinkedHashMap<>();
        res.put("stauts", resStatus);
        return res;
    }
}

package com.alibaba.otter.canal.adapter.launcher.rest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;

@RestController
public class CommonRest {

    private ExtensionLoader<OuterAdapter> loader;

    @PostConstruct
    public void init() {
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);
    }

    /**
     * Demo: curl http://127.0.0.1:8081/etl/hbase/mytest_person2.yml -X POST -d "params=0,1,2"
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
}

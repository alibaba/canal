package com.alibaba.otter.canal.adapter.launcher.rest;

import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;

@RestController
public class EtlRest {

    private ExtensionLoader<OuterAdapter> loader;

    @PostConstruct
    public void init() {
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);
    }

    /**
     * Demo: http://127.0.0.1:8081/etl/hbase/mytest_person2.yml
     */
    @GetMapping("/etl/{type}/{task}")
    public EtlResult etl(@PathVariable String type, @PathVariable String task,
                         @RequestParam(required = false) String params) {
        OuterAdapter adapter = loader.getExtension(type);
        List<String> paramArr = null;
        if (params != null) {
            String[] parmaArray = params.trim().split(";");
            paramArr = Arrays.asList(parmaArray);
        }

        return  adapter.etl(task, paramArr);
    }
}

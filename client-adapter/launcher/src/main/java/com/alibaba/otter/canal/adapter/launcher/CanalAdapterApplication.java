package com.alibaba.otter.canal.adapter.launcher;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class CanalAdapterApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(CanalAdapterApplication.class).run(args);
    }
}

package com.alibaba.otter.canal.adapter.launcher;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * 启动入口
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@SpringBootApplication
public class CanalAdapterApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(CanalAdapterApplication.class).run(args);
    }
}

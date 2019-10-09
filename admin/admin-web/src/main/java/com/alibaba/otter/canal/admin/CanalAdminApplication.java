package com.alibaba.otter.canal.admin;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 启动入口
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@SpringBootApplication
public class CanalAdminApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(CanalAdminApplication.class);
        application.setBannerMode(Banner.Mode.OFF);
        application.run(args);
    }
}

package com.alibaba.otter.canal.adapter.launcher;

import com.alibaba.otter.canal.adapter.launcher.monitor.AdapterRemoteConfigMonitor;
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
public class CanalAdapterApplication {

    public static void main(String[] args) {
        // 加载远程配置
//            String jdbcUrl = env.getProperty("canal.manager.jdbc.url");
//            if (StringUtils.isNotEmpty(jdbcUrl)) {
//                String jdbcUsername = env.getProperty("canal.manager.jdbc.username");
//                String jdbcPassword = env.getProperty("canal.manager.jdbc.password");
//                configMonitor = new AdapterRemoteConfigMonitor(jdbcUrl, jdbcUsername, jdbcPassword);
//                configMonitor.loadRemoteConfig();
//                configMonitor.loadRemoteAdapterConfigs();
//                contextRefresher.refresh();
//                configMonitor.start();
//            }

//        AdapterRemoteConfigMonitor aa  = new AdapterRemoteConfigMonitor();


        SpringApplication application = new SpringApplication(CanalAdapterApplication.class);
        application.setBannerMode(Banner.Mode.OFF);
        application.run(args);
    }
}

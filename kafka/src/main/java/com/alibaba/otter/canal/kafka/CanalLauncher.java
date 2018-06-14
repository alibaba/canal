package com.alibaba.otter.canal.kafka;

import com.alibaba.otter.canal.kafka.producer.CanalKafkaStarter;

/**
 * canal-kafka独立版本启动的入口类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalLauncher {

    public static void main(String[] args) {
        CanalServerStarter.init();
        CanalKafkaStarter.init();
    }
}

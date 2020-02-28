package com.alibaba.otter.canal.connector.kafka.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.ExtensionLoader;

@Ignore
public class CanalKafkaProducerTest {

    @Test
    public void testLoadKafkaProducer() throws IOException {
        Properties pro = new Properties();
        FileInputStream in = new FileInputStream("../../deployer/src/main/resources/canal.properties");
        pro.load(in);

        ExtensionLoader<CanalMQProducer> loader = ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
        CanalMQProducer canalMQProducer = loader.getExtension("kafka",
            "/../../deployer/target/canal/plugin",
            "/../../deployer/target/canal/plugin");
        canalMQProducer.init(pro);

        in.close();
    }
}

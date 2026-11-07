package com.alibaba.otter.canal.server;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;

public class CanalMQStarterTest {

    private CanalServerWithEmbedded server;
    private CanalMQStarter          starter;

    @Before
    public void setUp() {
        server = CanalServerWithEmbedded.instance();
        server.start();
        starter = new CanalMQStarter(new NoopMQProducer());
    }

    @After
    public void tearDown() {
        starter.destroy();
        server.stop();
    }

    /**
     * A destination listed in canal.destinations is started by start(). Stopping it later, e.g. when its
     * instance.properties is reloaded, must not fail.
     */
    @Test(timeout = 30000)
    public void testStopDestinationStartedAtStartup() throws InterruptedException {
        starter.start("example");
        // let the MQ worker of the destination get going
        Thread.sleep(500);

        starter.stopDestination("example");
    }

    private static class NoopMQProducer implements CanalMQProducer {

        private final MQProperties mqProperties = new MQProperties();

        @Override
        public void init(Properties properties) {
        }

        @Override
        public MQProperties getMqProperties() {
            return mqProperties;
        }

        @Override
        public void send(MQDestination canalDestination, Message message, Callback callback) {
        }

        @Override
        public void stop() {
        }
    }
}

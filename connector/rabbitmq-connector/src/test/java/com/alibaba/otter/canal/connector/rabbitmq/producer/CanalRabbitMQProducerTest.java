package com.alibaba.otter.canal.connector.rabbitmq.producer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import com.alibaba.otter.canal.connector.core.producer.AbstractMQProducer;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.connector.rabbitmq.config.RabbitMQProducerConfig;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.Message;
import com.rabbitmq.client.Channel;

public class CanalRabbitMQProducerTest {

    /**
     * Exercises the full send() -> sendMessage() -> channel.basicPublish() path with a
     * dynamic-topic configuration, asserting the routing key passed to basicPublish
     * retains its dot (instead of being mangled into an underscore at the call site).
     * Guards against the call site being reverted to entry.getKey().replace('.', '_').
     */
    @Test
    public void testDynamicTopicRoutingKeyRetainsDots() throws Exception {
        // Build a canal Message containing one ROWDATA entry on schema retl / table retl_mark,
        // which is matched by the dynamic-topic rule below.
        Header header = Header.newBuilder().setSchemaName("retl").setTableName("retl_mark").build();
        Entry entry = Entry.newBuilder().setHeader(header).setEntryType(EntryType.ROWDATA).build();
        Message message = new Message(1L, Collections.singletonList(entry));

        // dynamic-topic rule: route retl.retl_mark to the literal topic "canal.sync" (contains a dot).
        // The part before ':' is the topic/routing key, the part after ':' is the match config.
        MQDestination destination = new MQDestination();
        destination.setCanalDestination("example");
        destination.setTopic("canal.default");
        destination.setDynamicTopic("canal.sync:retl.retl_mark");

        CanalRabbitMQProducer producer = new CanalRabbitMQProducer();

        // Inject a non-flat RabbitMQ config so send() takes the non-flat branch (no buildExecutor needed).
        RabbitMQProducerConfig config = new RabbitMQProducerConfig();
        config.setFlatMessage(false);
        config.setExchange("canal.exchange");
        setField(producer, getDeclaredField(AbstractMQProducer.class, "mqProperties"), config);

        // Replace the send executor with a synchronous single-thread one so that basicPublish runs
        // (and completes) before send() returns, making the verify deterministic.
        ThreadPoolExecutor syncExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(4), r -> {
                Thread t = new Thread(r, "test-sync-sender");
                t.setDaemon(true);
                return t;
            }, new ThreadPoolExecutor.CallerRunsPolicy());
        setField(producer, getDeclaredField(AbstractMQProducer.class, "sendExecutor"), syncExecutor);

        // Mock the RabbitMQ channel (private field, no injection point in production code).
        Channel mockChannel = mock(Channel.class);
        setField(producer, getDeclaredField(CanalRabbitMQProducer.class, "channel"), mockChannel);

        Callback callback = Mockito.mock(Callback.class);

        producer.send(destination, message, callback);

        // basicPublish(exchange, routingKey, props, body): the 2nd argument is the routing key.
        // It must retain the dot from "canal.sync" and not be flattened to "canal_sync".
        verify(mockChannel).basicPublish(eq("canal.exchange"), eq("canal.sync"), any(), any());
        // also assert the routing key was never published in its underscore-mangled form
        Mockito.verify(mockChannel, Mockito.never()).basicPublish(anyString(), eq("canal_sync"), any(), any());

        syncExecutor.shutdownNow();
    }

    private static Field getDeclaredField(Class<?> clazz, String name) throws NoSuchFieldException {
        Field f = clazz.getDeclaredField(name);
        f.setAccessible(true);
        return f;
    }

    private static void setField(Object target, Field field, Object value) throws IllegalAccessException {
        field.set(target, value);
    }
}

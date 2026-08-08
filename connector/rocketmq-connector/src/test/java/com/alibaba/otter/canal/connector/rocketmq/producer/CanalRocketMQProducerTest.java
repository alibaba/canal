package com.alibaba.otter.canal.connector.rocketmq.producer;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.connector.rocketmq.config.RocketMQProducerConfig;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.Header;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.Type;
import com.alibaba.otter.canal.protocol.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * CanalRocketMQProducer.send() 四条分支路由的单元测试。
 *
 * <pre>
 * 覆盖关于新增dynamicTag相关改动涉及的四个分支：
 *   1. dynamicTopic + dynamicTag （sendByDynamicTag 二次分片）
 *   2. dynamicTopic only          （按 topic 路由，使用默认 tag）
 *   3. dynamicTag only            （sendByDynamicTag 按 tag 分片）
 *   4. neither                    （直接发送到默认 topic + 默认 tag）
 *
 * 策略
 *   - 反射注入一个 mock(DefaultMQProducer) 到 defaultMQProducer 字段 —— mock 真正的 I/O 边界；
 *   - 不 mock MQMessageUtils.messageTopics，直接构造带 schema/table 的真实 Entry，
 *     使 messageTopics 真实执行路由 —— 路由逻辑本身也被测试覆盖；
 *   - 用 ArgumentCaptor 捕获 defaultMQProducer.send 收到的 RocketMQ Message，
 *     断言其 topic / tag 是否符合各分支预期。
 *
 * 运行方式（无需真实 RocketMQ）：
 * <pre>
 * mvn test -pl connector/rocketmq-connector -Dtest=CanalRocketMQProducerTest -Dmaven.test.skip=false
 * </pre>
 *
 * @author yongchang.wang
 */
public class CanalRocketMQProducerTest {

    private static final String DEFAULT_TOPIC        = "canal_topic_default";
    private static final String DEFAULT_TAG          = "canal_tag";
    /** 命中 mytest1.user 时 -> topic = topic_user */
    private static final String DYNAMIC_TOPIC_CONFIG = "topic_user:mytest1.user";
    /** 命中 mytest1.user 时 -> tag = tag_user */
    private static final String DYNAMIC_TAG_CONFIG   = "tag_user:mytest1.user";

    private CanalRocketMQProducer producer;
    private DefaultMQProducer     mockProducer;
    private ThreadPoolExecutor    sendExecutor;
    private ThreadPoolExecutor    buildExecutor;

    @Before
    public void setUp() throws Exception {
        producer = new CanalRocketMQProducer();

        // 1. mock 真正的 I/O 边界 DefaultMQProducer，反射注入到 private 字段
        mockProducer = Mockito.mock(DefaultMQProducer.class);
        setField(producer, "defaultMQProducer", mockProducer);

        // 2. 注入必要的配置与线程池（AbstractMQProducer 的 protected 字段，跨包用反射）
        RocketMQProducerConfig mqProperties = new RocketMQProducerConfig();
        mqProperties.setTag(DEFAULT_TAG);
        mqProperties.setFlatMessage(false); // 走非 flat 的单条发送路径，便于捕获 Message
        setField(producer, "mqProperties", mqProperties);

        sendExecutor = new ThreadPoolExecutor(2,
            2,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(4));
        buildExecutor = new ThreadPoolExecutor(2,
            2,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(4));
        setField(producer, "sendExecutor", sendExecutor);
        setField(producer, "buildExecutor", buildExecutor);
    }

    @After
    public void tearDown() {
        if (sendExecutor != null) {
            sendExecutor.shutdownNow();
        }
        if (buildExecutor != null) {
            buildExecutor.shutdownNow();
        }
    }

    // ====================== 分支 1：dynamicTopic + dynamicTag ======================
    @Test
    public void testSendDynamicTopicAndTag() throws Exception {
        MQDestination dest = buildDestination(DYNAMIC_TOPIC_CONFIG, DYNAMIC_TAG_CONFIG);
        Message message = buildMessage("mytest1", "user");
        TestCallback callback = new TestCallback();

        producer.send(dest, message, callback);

        assertTrue("成功路径应调用 commit", callback.committed);
        org.apache.rocketmq.common.message.Message sent = captureSingleSent();
        assertEquals("topic 应被动态路由为 topic_user", "topic_user", sent.getTopic());
        assertEquals("tag 应被动态路由为 tag_user", "tag_user", sent.getTags());
    }

    // ====================== 分支 2：仅 dynamicTopic ======================
    @Test
    public void testSendDynamicTopicOnly() throws Exception {
        MQDestination dest = buildDestination(DYNAMIC_TOPIC_CONFIG, null);
        Message message = buildMessage("mytest1", "user");
        TestCallback callback = new TestCallback();

        producer.send(dest, message, callback);

        assertTrue("成功路径应调用 commit", callback.committed);
        org.apache.rocketmq.common.message.Message sent = captureSingleSent();
        assertEquals("topic 应被动态路由为 topic_user", "topic_user", sent.getTopic());
        assertEquals("tag 应为默认 tag", DEFAULT_TAG, sent.getTags());
    }

    // ====================== 分支 3：仅 dynamicTag ======================
    @Test
    public void testSendDynamicTagOnly() throws Exception {
        MQDestination dest = buildDestination(null, DYNAMIC_TAG_CONFIG);
        Message message = buildMessage("mytest1", "user");
        TestCallback callback = new TestCallback();

        producer.send(dest, message, callback);

        assertTrue("成功路径应调用 commit", callback.committed);
        org.apache.rocketmq.common.message.Message sent = captureSingleSent();
        assertEquals("topic 应为默认 topic", DEFAULT_TOPIC, sent.getTopic());
        assertEquals("tag 应被动态路由为 tag_user", "tag_user", sent.getTags());
    }

    // ====================== 分支 4：无 dynamicTopic，无 dynamicTag ======================
    @Test
    public void testSendNeitherTopicNorTag() throws Exception {
        MQDestination dest = buildDestination(null, null);
        Message message = buildMessage("mytest1", "user");
        TestCallback callback = new TestCallback();

        producer.send(dest, message, callback);

        assertTrue("成功路径应调用 commit", callback.committed);
        assertFalse("成功路径不应 rollback", callback.rolledBack);
        org.apache.rocketmq.common.message.Message sent = captureSingleSent();
        assertEquals("topic 应为默认 topic", DEFAULT_TOPIC, sent.getTopic());
        assertEquals("tag 应为默认 tag", DEFAULT_TAG, sent.getTags());
    }

    // ====================== 异常路径：发送失败应 rollback（间接验证 waitForResult 传播异常）======================
    @Test
    public void testSendFailureTriggersRollback() throws Exception {
        // 让底层 producer.send 抛异常（走 dynamicTopic 路径，经 template.submit + waitForResult）
        when(mockProducer.send(any(org.apache.rocketmq.common.message.Message.class),
            any(MessageQueueSelector.class),
            any())).thenThrow(new RuntimeException("mock send error"));

        MQDestination dest = buildDestination(DYNAMIC_TOPIC_CONFIG, null);
        Message message = buildMessage("mytest1", "user");
        TestCallback callback = new TestCallback();

        producer.send(dest, message, callback);

        assertTrue("异常路径应调用 rollback", callback.rolledBack);
        assertFalse("异常路径不应 commit", callback.committed);
    }

    // ======================= 辅助方法 =======================

    /** 捕获 defaultMQProducer.send 收到的单条 RocketMQ Message（断言仅发送一次）。 */
    private org.apache.rocketmq.common.message.Message captureSingleSent() throws Exception {
        ArgumentCaptor<org.apache.rocketmq.common.message.Message> captor = ArgumentCaptor.forClass(org.apache.rocketmq.common.message.Message.class);
        verify(mockProducer, times(1)).send(captor.capture(), any(MessageQueueSelector.class), any());
        return captor.getValue();
    }

    private MQDestination buildDestination(String dynamicTopic, String dynamicTag) {
        MQDestination d = new MQDestination();
        d.setTopic(DEFAULT_TOPIC);
        d.setPartition(0);
        d.setPartitionsNum(1);
        if (dynamicTopic != null) {
            d.setDynamicTopic(dynamicTopic);
        }
        if (dynamicTag != null) {
            d.setDynamicTag(dynamicTag);
        }
        return d;
    }

    /** 构造一条含 schema/table 的 canal Message，驱动 MQMessageUtils.messageTopics 真实路由。 */
    private Message buildMessage(String schema, String table) {
        Entry entry = Entry.newBuilder()
            .setHeader(Header.newBuilder()
                .setSchemaName(schema)
                .setTableName(table)
                .setExecuteTime(System.currentTimeMillis())
                .setEventType(EventType.INSERT)
                .setSourceType(Type.MYSQL)
                .build())
            .setEntryType(EntryType.ROWDATA)
            .setStoreValue(RowChange.newBuilder()
                .setEventType(EventType.INSERT)
                .setIsDdl(false)
                .addRowDatas(RowData.newBuilder()
                    .addAfterColumns(Column.newBuilder()
                        .setName("id")
                        .setValue("1")
                        .setIsKey(true)
                        .setSqlType(Types.INTEGER)
                        .setMysqlType("int(11)")
                        .setIsNull(false)
                        .setUpdated(true)
                        .build())
                    .build())
                .build()
                .toByteString())
            .build();
        return new Message(System.nanoTime(), Collections.singletonList(entry));
    }

    /** 反射设置字段（沿继承链查找，兼容 private 与父类 protected 字段）。 */
    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    /** 记录 commit / rollback 调用，用于断言 send() 的回调走向。 */
    private static class TestCallback implements Callback {

        volatile boolean committed = false;
        volatile boolean rolledBack = false;

        @Override
        public void commit() {
            committed = true;
        }

        @Override
        public void rollback() {
            rolledBack = true;
        }
    }
}

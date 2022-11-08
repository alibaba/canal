package com.alibaba.otter.canal.server.embedded;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.CanalHASwitchable;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
@Ignore
public abstract class BaseCanalServerWithEmbededTest {

    protected static final String   cluster1       = "127.0.0.1:2188";
    protected static final String   DESTINATION    = "example";
    protected static final String   DETECTING_SQL  = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    protected static final String   MYSQL_ADDRESS  = "127.0.0.1";
    protected static final String   USERNAME       = "canal";
    protected static final String   PASSWORD       = "canal";
    protected static final String   FILTER         = ".*\\\\..*";

    private CanalServerWithEmbedded server;
    private ClientIdentity          clientIdentity = new ClientIdentity(DESTINATION, (short) 1);                               ;

    @Before
    public void setUp() {
        server = CanalServerWithEmbedded.instance();
        server.setCanalInstanceGenerator(destination -> {
            Canal canal = buildCanal();
            return new CanalInstanceWithManager(canal, FILTER);
        });
        server.start();
        server.start(DESTINATION);
    }

    @After
    public void tearDown() {
        server.stop();
    }

    @Test
    public void testGetWithoutAck() {
        int maxEmptyCount = 10;
        int emptyCount = 0;
        int totalCount = 0;
        server.subscribe(clientIdentity);
        while (emptyCount < maxEmptyCount) {
            Message message = server.getWithoutAck(clientIdentity, 11);
            if (CollectionUtils.isEmpty(message.getEntries())) {
                emptyCount++;
                try {
                    Thread.sleep(emptyCount * 300L);
                } catch (InterruptedException e) {
                    Assert.fail();
                }

                System.out.println("empty count : " + emptyCount);
            } else {
                emptyCount = 0;
                totalCount += message.getEntries().size();
                server.ack(clientIdentity, message.getId());
            }
        }

        System.out.println("!!!!!! testGetWithoutAck totalCount : " + totalCount);
        server.unsubscribe(clientIdentity);
    }

    @Test
    public void testGet() {
        int maxEmptyCount = 10;
        int emptyCount = 0;
        int totalCount = 0;
        server.subscribe(clientIdentity);
        while (emptyCount < maxEmptyCount) {
            Message message = server.get(clientIdentity, 11);
            if (CollectionUtils.isEmpty(message.getEntries())) {
                emptyCount++;
                try {
                    Thread.sleep(emptyCount * 300L);
                } catch (InterruptedException e) {
                    Assert.fail();
                }

                System.out.println("empty count : " + emptyCount);
            } else {
                emptyCount = 0;
                totalCount += message.getEntries().size();
            }
        }

        System.out.println("!!!!!! testGet totalCount : " + totalCount);
        server.unsubscribe(clientIdentity);
    }

    // @Test
    public void testRollback() {
        int maxEmptyCount = 10;
        int emptyCount = 0;
        int totalCount = 0;
        server.subscribe(clientIdentity);
        while (emptyCount < maxEmptyCount) {
            Message message = server.getWithoutAck(clientIdentity, 11);
            if (CollectionUtils.isEmpty(message.getEntries())) {
                emptyCount++;
                try {
                    Thread.sleep(emptyCount * 300L);
                } catch (InterruptedException e) {
                    Assert.fail();
                }

                System.out.println("empty count : " + emptyCount);
            } else {
                emptyCount = 0;
                totalCount += message.getEntries().size();
            }
        }
        System.out.println("!!!!!! testRollback totalCount : " + totalCount);

        server.rollback(clientIdentity);// 直接rollback掉，再取一次
        emptyCount = 0;
        totalCount = 0;
        while (emptyCount < maxEmptyCount) {
            Message message = server.getWithoutAck(clientIdentity, 11);
            if (CollectionUtils.isEmpty(message.getEntries())) {
                emptyCount++;
                try {
                    Thread.sleep(emptyCount * 300L);
                } catch (InterruptedException e) {
                    Assert.fail();
                }

                System.out.println("empty count : " + emptyCount);
            } else {
                emptyCount = 0;
                totalCount += message.getEntries().size();
            }
        }

        System.out.println("!!!!!! testRollback after rollback ,  totalCount : " + totalCount);
        server.unsubscribe(clientIdentity);
    }

    // @Test
    public void testSwitch() {
        int maxEmptyCount = 10;
        int emptyCount = 0;
        int totalCount = 0;

        int thresold = 50;
        int batchSize = 11;
        server.subscribe(clientIdentity);
        while (emptyCount < maxEmptyCount) {
            Message message = server.get(clientIdentity, batchSize);
            if (CollectionUtils.isEmpty(message.getEntries())) {
                emptyCount++;
                try {
                    Thread.sleep(emptyCount * 300L);
                } catch (InterruptedException e) {
                    Assert.fail();
                }

                System.out.println("empty count : " + emptyCount);
            } else {
                emptyCount = 0;
                totalCount += message.getEntries().size();

                if ((totalCount + 1) % 100 >= thresold && (totalCount + 1) % 100 <= thresold + batchSize) {
                    CanalEventParser eventParser = server.getCanalInstances().get(DESTINATION).getEventParser();
                    if (eventParser instanceof CanalHASwitchable) {
                        ((CanalHASwitchable) eventParser).doSwitch();// 执行切换
                        try {
                            Thread.sleep(5 * 1000); // 等待parser启动
                        } catch (InterruptedException e) {
                            Assert.fail();
                        }
                    }
                }
            }
        }

        System.out.println("!!!!!! testGet totalCount : " + totalCount);
        server.unsubscribe(clientIdentity);
    }

    abstract protected Canal buildCanal();
}

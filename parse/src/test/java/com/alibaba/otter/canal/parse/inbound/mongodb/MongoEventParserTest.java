package com.alibaba.otter.canal.parse.inbound.mongodb;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.BsonConverter;
import com.alibaba.otter.canal.parse.inbound.mongodb.dbsync.ChangeStreamEvent;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Ignore;


@Ignore
public class MongoEventParserTest {

    @Test
    public void testParse() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3000 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final MongoEventParser mongoEventParser = new MongoEventParser();

        // 注意：mongodb需开启副本集模式
        // bin/mongod -f ../conf/mongodb.conf --replSet rs0
        // bin/mongo
        // rs.initiate({_id:"rs0", members:[{_id:0,host:'127.0.0.1:27017'}]});
        final MongoAuthenticationInfo authenticationInfo =
                new MongoAuthenticationInfo("127.0.0.1:27017", "sa", "sa");

        final EntryPosition specifiedPosition = buildPosition(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));

        mongoEventParser.setAuthenticationInfo(authenticationInfo);
        mongoEventParser.setSpecifiedPosition(specifiedPosition);

        mongoEventParser.setEventSink(new AbstractCanalEventSinkTest<List<CanalEntry.Entry>>() {
            @Override
            public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination)
                    throws CanalSinkException {
                for (CanalEntry.Entry entry : entrys) {
                    if (entry.getEntryType() != CanalEntry.EntryType.HEARTBEAT) {
                        entryCount.incrementAndGet();
                        String logfilename = entry.getHeader().getLogfileName();
                        long logfileoffset = entry.getHeader().getLogfileOffset();
                        long executeTime = entry.getHeader().getExecuteTime();
                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);
                        break;
                    }
                }

                if (entryCount.get() > 0) {
                    mongoEventParser.stop();
                    timeoutChecker.stop();
                    timeoutChecker.touch();
                }
                return true;
            }
        });

        mongoEventParser.setLogPositionManager(new AbstractLogPositionManager() {

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }

            @Override
            public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
                System.out.println(logPosition);
            }
        });

        mongoEventParser.start();

        timeoutChecker.waitForIdle();

        if (mongoEventParser.isStart()) {
            mongoEventParser.stop();
        }

        // 直接sink
        Assert.assertTrue(specifiedPosition.getPosition().compareTo(entryPosition.getPosition()) < 0);

        // check
        Assert.assertTrue(entryCount.get() > 0);

    }

    private EntryPosition buildPosition(Long timestamp) {
        return new EntryPosition(ChangeStreamEvent.LOG_FILE_COLLECTION,
                BsonConverter.convertTime(timestamp).getValue(), timestamp);
    }
}
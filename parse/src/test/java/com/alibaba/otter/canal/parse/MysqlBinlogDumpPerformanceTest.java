package com.alibaba.otter.canal.parse;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Ignore;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;

@Ignore
public class MysqlBinlogDumpPerformanceTest {

    public static void main(String args[]) {
        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition startPosition = new EntryPosition("mysql-bin.000007", 89796293L, 100L);
        controller.setConnectionCharset("UTF-8");
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setFilterQueryDml(true);
        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress("100.81.154.142", 3306), "canal", "canal"));
        controller.setMasterPosition(startPosition);
        controller.setEnableTsdb(false);
        controller.setDestination("example");
        controller.setTsdbSpringXml("classpath:spring/tsdb/h2-tsdb.xml");
        // controller.setEventFilter(new AviaterRegexFilter("test\\..*"));
        // controller.setEventBlackFilter(new
        // AviaterRegexFilter("canal_tsdb\\..*"));
        controller.setParallel(true);
        controller.setParallelBufferSize(256);
        controller.setParallelThreadSize(16);
        controller.setIsGTIDMode(false);
        final AtomicLong sum = new AtomicLong(0);
        final AtomicLong last = new AtomicLong(0);
        final AtomicLong start = new AtomicLong(System.currentTimeMillis());
        final AtomicLong end = new AtomicLong(0);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<CanalEntry.Entry>>() {

            public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                                   throws CanalSinkException,
                                                                                                                   InterruptedException {

                sum.addAndGet(entrys.size());
                long current = sum.get();
                if (current - last.get() >= 100000) {
                    end.set(System.currentTimeMillis());
                    long tps = ((current - last.get()) * 1000) / (end.get() - start.get());
                    System.out.println(" total : " + sum + " , cost : " + (end.get() - start.get()) + " , tps : " + tps);
                    last.set(current);
                    start.set(end.get());
                }
                return true;
            }

        });
        controller.setLogPositionManager(new AbstractLogPositionManager() {

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }

            @Override
            public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
            }
        });

        controller.start();

        try {
            Thread.sleep(100 * 1000 * 1000L);
        } catch (InterruptedException e) {
        }
        controller.stop();
    }

    public static abstract class AbstractCanalEventSinkTest<T> extends AbstractCanalLifeCycle implements CanalEventSink<T> {

        public void interrupt() {
        }
    }
}

package com.alibaba.otter.canal.server.embedded;

import java.net.InetSocketAddress;
import java.util.Arrays;

import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.*;
import org.junit.Ignore;

@Ignore
public class CanalServerWithEmbedded_FileModeTest extends BaseCanalServerWithEmbededTest {

    protected Canal buildCanal() {
        Canal canal = new Canal();
        canal.setId(1L);
        canal.setName(DESTINATION);
        canal.setDesc("my standalone server test ");

        CanalParameter parameter = new CanalParameter();

        parameter.setMetaMode(MetaMode.LOCAL_FILE);
        parameter.setDataDir("./conf");
        parameter.setMetaFileFlushPeriod(1000);
        parameter.setHaMode(HAMode.HEARTBEAT);
        parameter.setIndexMode(IndexMode.MEMORY_META_FAILBACK);

        parameter.setStorageMode(StorageMode.MEMORY);
        parameter.setMemoryStorageBufferSize(32 * 1024);

        parameter.setSourcingType(SourcingType.MYSQL);
        parameter.setDbAddresses(Arrays.asList(new InetSocketAddress(MYSQL_ADDRESS, 3306),
            new InetSocketAddress(MYSQL_ADDRESS, 3306)));
        parameter.setDbUsername(USERNAME);
        parameter.setDbPassword(PASSWORD);
        parameter.setPositions(Arrays.asList("{\"journalName\":\"mysql-bin.000001\",\"position\":332L,\"timestamp\":\"1505998863000\"}",
            "{\"journalName\":\"mysql-bin.000001\",\"position\":332L,\"timestamp\":\"1505998863000\"}"));

        parameter.setSlaveId(1234L);

        parameter.setDefaultConnectionTimeoutInSeconds(30);
        parameter.setConnectionCharset("UTF-8");
        parameter.setConnectionCharsetNumber((byte) 33);
        parameter.setReceiveBufferSize(8 * 1024);
        parameter.setSendBufferSize(8 * 1024);

        parameter.setDetectingEnable(false);
        parameter.setDetectingIntervalInSeconds(10);
        parameter.setDetectingRetryTimes(3);
        parameter.setDetectingSQL(DETECTING_SQL);

        canal.setCanalParameter(parameter);
        return canal;
    }
}

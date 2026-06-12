package com.alibaba.otter.canal.client.adapter.clickhouse.service;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MirrorDbConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public class ClickHouseMirrorDbBatchSyncServiceTest {

    @Test
    public void updateShouldReuseDmlRoutingInMirrorDbMode() {
        Map<String, MirrorDbConfig> mirrorDbConfigCache = new ConcurrentHashMap<>();
        MappingConfig mappingConfig = new MappingConfig();
        mappingConfig.setDestination("example");
        MappingConfig.DbMapping dbMapping = new MappingConfig.DbMapping();
        dbMapping.setMirrorDb(true);
        dbMapping.setDatabase("mytest");
        mappingConfig.setDbMapping(dbMapping);
        mirrorDbConfigCache.put("example.mytest", MirrorDbConfig.create("mytest.yml", mappingConfig));

        ClickHouseMirrorDbBatchSyncService mirrorDbBatchSyncService = new ClickHouseMirrorDbBatchSyncService(
            mirrorDbConfigCache, null, 0, null, null, new ConcurrentHashMap<>(), false);
        RecordingClickHouseBatchSyncService batchSyncService = new RecordingClickHouseBatchSyncService();
        setField(mirrorDbBatchSyncService, "clickHouseBatchSyncService", batchSyncService);

        Dml dml = new Dml();
        dml.setDestination("example");
        dml.setTs(new Date().getTime());
        dml.setType("UPDATE");
        dml.setDatabase("mytest");
        dml.setTable("user");
        dml.setPkNames(Collections.singletonList("id"));
        List<Map<String, Object>> dataList = Collections.singletonList(new LinkedHashMap<>());
        dataList.get(0).put("id", 1L);
        dataList.get(0).put("name", "Eric2");
        dml.setData(dataList);
        List<Map<String, Object>> oldList = Collections.singletonList(new LinkedHashMap<>());
        oldList.get(0).put("name", "Eric");
        dml.setOld(oldList);

        mirrorDbBatchSyncService.sync(Collections.singletonList(dml));

        Assert.assertEquals(1, batchSyncService.distributedDmls.size());
        Assert.assertSame(dml, batchSyncService.distributedDmls.get(0));
    }

    private static void setField(Object target, String fieldName, Object value) {
        try {
            Field field = ClickHouseMirrorDbBatchSyncService.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static class RecordingClickHouseBatchSyncService extends ClickHouseBatchSyncService {

        private final List<Dml> distributedDmls = new ArrayList<>();

        private RecordingClickHouseBatchSyncService() {
            super(null, 0, null, null, new ConcurrentHashMap<>(), false);
        }

        @Override
        public void sync(List<Dml> dmls, java.util.function.Function<Dml, Boolean> function) {
            boolean toExecute = false;
            for (Dml dml : dmls) {
                if (!toExecute) {
                    toExecute = function.apply(dml);
                } else {
                    function.apply(dml);
                }
            }
        }

        @Override
        void distributeDml(MappingConfig config, Dml dml) {
            distributedDmls.add(dml);
        }
    }
}

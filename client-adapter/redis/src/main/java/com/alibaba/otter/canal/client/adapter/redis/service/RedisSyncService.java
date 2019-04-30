package com.alibaba.otter.canal.client.adapter.redis.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.redis.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis 同步操作业务
 *
 * @version 1.0.0
 */
public class RedisSyncService {
    private static final Logger logger = LoggerFactory.getLogger(RedisSyncService.class);
    private RedisService redisService;

    public RedisSyncService(RedisService redisService) {
        this.redisService = redisService;
    }

    public void sync(Map<String, MappingConfig> configMap, Dml dml) {
        if (configMap == null || configMap.values().isEmpty()) {
            return;
        }

        for (MappingConfig config : configMap.values()) {
            MappingConfig.RedisMapping redisMapping = config.getRedisMapping();
            String type = dml.getType();

            Map<String, String> dataMap = new HashMap<>();

            dml.getData().forEach((value) -> {
                Map<String, Object> values = new HashMap<>();
                String pk = redisMapping.getPk();
                Object pkValue = value.get(pk);
                if (pkValue == null && pk != null) {
                    logger.warn("The pk value is not available: `{}`.`{}` column: {}", redisMapping.getDatabase(), redisMapping.getTable(), pk);
                    return;
                }

                SyncUtil.getColumnsMap(redisMapping, value)
                        .forEach((targetColumn, srcColumn) -> values.put(targetColumn, value.get(srcColumn)));

                String jsonString = JSON.toJSONString(values, SerializerFeature.WriteMapNullValue);
                dataMap.put(redisMapping.getKey().replace("{}", String.valueOf(pkValue)), jsonString);
            });

            if (type != null && (type.equalsIgnoreCase("INSERT") || type.equalsIgnoreCase("UPDATE"))) {
                update(dataMap, redisMapping.getExpire());
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(dataMap);
            }
        }
    }

    private void update(Map<String, String> dataMap, int expire) {
        //TODO 批量处理优化
        dataMap.forEach((key, value) -> {
            Object result;
            result = redisService.set(key.getBytes(), expire, value.getBytes());

            logger.info("set key: {} value: {} expire: {} result: {}", key, value, expire, result);
        });
    }

    private void delete(Map<String, String> dataMap) {
        String keys = String.join(" ", dataMap.keySet());
        Object result = redisService.del(keys);
        logger.info("del keys: {} result: {}", keys, result);
    }
}

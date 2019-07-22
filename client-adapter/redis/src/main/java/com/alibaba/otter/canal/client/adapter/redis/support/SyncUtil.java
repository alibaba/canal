package com.alibaba.otter.canal.client.adapter.redis.support;

import com.alibaba.otter.canal.client.adapter.redis.config.MappingConfig;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class SyncUtil {
    public static Map<String, String> getColumnsMap(MappingConfig.RedisMapping redisMapping, Map<String, Object> data) {
        return getColumnsMap(redisMapping, data.keySet());
    }

    public static Map<String, String> getColumnsMap(MappingConfig.RedisMapping redisMapping, Collection<String> columns) {
        Map<String, String> columnsMap;
        if (redisMapping.getMapAll()) {
            if (redisMapping.getAllMapColumns() != null) {
                return redisMapping.getAllMapColumns();
            }
            columnsMap = new LinkedHashMap<>();
            for (String srcColumn : columns) {
                boolean flag = true;
                if (redisMapping.getTargetColumns() != null) {
                    for (Map.Entry<String, String> entry : redisMapping.getTargetColumns().entrySet()) {
                        if (srcColumn.equals(entry.getValue())) {
                            columnsMap.put(entry.getKey(), srcColumn);
                            flag = false;
                            break;
                        }
                    }
                }
                if (flag) {
                    columnsMap.put(srcColumn, srcColumn);
                }
            }
            redisMapping.setAllMapColumns(columnsMap);
        } else {
            columnsMap = redisMapping.getTargetColumns();
        }
        return columnsMap;
    }
}

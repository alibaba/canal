package com.alibaba.otter.canal.client.adapter.rdb.support;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.rdb.config.MappingConfig;

public class SyncUtil {

    public static Map<String, String> getColumnsMap(MappingConfig.DbMapping dbMapping, Map<String, Object> data) {
        return getColumnsMap(dbMapping, data.keySet());
    }

    public static Map<String, String> getColumnsMap(MappingConfig.DbMapping dbMapping, Collection<String> columns) {
        Map<String, String> columnsMap;
        if (dbMapping.isMapAll()) {
            columnsMap = new LinkedHashMap<>();
            for (String srcColumn : columns) {
                boolean flag = true;
                if (dbMapping.getTargetColumns() != null) {
                    for (Map.Entry<String, String> entry : dbMapping.getTargetColumns().entrySet()) {
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
        } else {
            columnsMap = dbMapping.getTargetColumns();
        }
        return columnsMap;
    }
}

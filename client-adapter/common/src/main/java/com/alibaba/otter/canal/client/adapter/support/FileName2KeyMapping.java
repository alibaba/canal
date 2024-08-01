package com.alibaba.otter.canal.client.adapter.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by @author zhuchao on @date 2021/11/11.
 */
public class FileName2KeyMapping {

    private static Map<String, String> MAP = new ConcurrentHashMap<>();

    public static void register(String type, String fileName, String key) {
        MAP.putIfAbsent(join(type, fileName), key);
    }

    public static void unregister(String type, String fileName) {
        MAP.remove(join(type, fileName));
    }

    public static String getKey(String type, String fileName) {
        return MAP.get(join(type, fileName));
    }

    private static String join(String type, String fileName) {
        return type + "|" + fileName;
    }
}

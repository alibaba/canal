package com.alibaba.otter.canal.common.utils;


public class MQUtil {
    /**
     * topic是否是正则表达式
     *
     * @param topic
     * @return true是正则表达式
     */
    public static boolean isPatternTopic(String topic) {
        return !topic.matches("^[0-9a-z:/-]+$");
    }

    /**
     * 检查topic有效性
     *
     * @param topic
     * @return
     */
    public static boolean checkTopic(String topic) {
        return topic.matches("^[0-9a-z:/.*-]+$");
    }

    /**
     * 检查tag有效性
     *
     * @param tag
     * @return
     */
    public static boolean checkTag(String tag) {
        return tag.matches("^[0-9a-zA-Z.*]+$");
    }

    /**
     * 判断tag是否是正则
     *
     * @param tag
     * @return
     */
    public static boolean isPatternTag(String tag) {
        return !tag.matches("^[0-9a-zA-Z]+$");
    }

    /**
     * 检查topic有效性
     *
     * @param topics
     */
    public static void checkTopicWithErr(String... topics) {
        if (null == topics || 0 == topics.length) {
            throw new NullPointerException("topic cannot null");
        }

        if (1 == topics.length) {
            boolean ok = checkTopic(topics[0]);
            if (ok) {
                return;
            }
            throw new RuntimeException("topic invalid: " + topics[0]);
        }

        for (String t : topics) {
            if (!checkTopic(t)) {
                throw new IllegalArgumentException("topic invalid: " + t);
            }
            if (isPatternTopic(t)) {
                throw new RuntimeException("pattern topic cannot multi: " + t);
            }
        }
    }

    /**
     * 检查tag有效性
     *
     * @param tags
     */
    public static void checkTagWithErr(String... tags) {
        // 空表示不使用tag
        if (null == tags || 0 == tags.length) {
            return;
        }

//        if (1 == tags.length && (null == tags[0] || 0 == tags[0].trim().length())) {
//            throw new NullPointerException("tag cannot null");
//        }

        for (String t : tags) {
            if (!checkTag(t)) {
                throw new IllegalArgumentException("tag invalid: " + t);
            }
            if (isPatternTag(t)) {
                throw new RuntimeException("pattern tag cannot multi: " + t);
            }
        }
    }
}

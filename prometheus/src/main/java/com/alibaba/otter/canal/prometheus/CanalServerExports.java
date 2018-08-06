package com.alibaba.otter.canal.prometheus;


/**
 * @author Chuanyi Li
 */
public class CanalServerExports {

    private static boolean initialized = false;

    public static synchronized void initialize() {
        if (!initialized) {
            initialized = true;
        }
    }

}

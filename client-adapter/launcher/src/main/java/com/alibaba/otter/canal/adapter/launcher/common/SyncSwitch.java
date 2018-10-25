package com.alibaba.otter.canal.adapter.launcher.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.adapter.launcher.config.AdapterCanalConfig;
import com.alibaba.otter.canal.common.utils.BooleanMutex;

@Component
public class SyncSwitch {

    private static final Map<String, BooleanMutex> LOCAL_LOCK = new ConcurrentHashMap<>();

    private Mode                                   mode       = Mode.LOCAL;

    @Resource
    private AdapterCanalConfig                     adapterCanalConfig;

    @PostConstruct
    public void init() {
        if (StringUtils.isEmpty(adapterCanalConfig.getZookeeperHosts())) {
            mode = Mode.LOCAL;
            LOCAL_LOCK.clear();
            for (String destination : AdapterCanalConfig.DESTINATIONS) {
                // 对应每个destination注册锁
                LOCAL_LOCK.put(destination, new BooleanMutex(true));
            }
        } else {
            mode = Mode.DISTRIBUTED;
        }
    }

    public synchronized void off(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null && mutex.state()) {
                mutex.set(false);
            }
        }
    }

    public synchronized void on(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null && !mutex.state()) {
                mutex.set(true);
            }
        }
    }

    public synchronized void release(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null && mutex.state()) {
                mutex.set(false);
            }
        }
    }

    public Boolean status(String destination) {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null) {
                return mutex.state();
            } else {
                return null;
            }
        }
        return null;
    }

    public void get(String destination) throws InterruptedException {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null) {
                mutex.get();
            }
        }
    }

    public void get(String destination, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        if (mode == Mode.LOCAL) {
            BooleanMutex mutex = LOCAL_LOCK.get(destination);
            if (mutex != null) {
                mutex.get(timeout, unit);
            }
        }
    }

    enum Mode {
               LOCAL, // 本地模式
               DISTRIBUTED // 分布式
    }
}

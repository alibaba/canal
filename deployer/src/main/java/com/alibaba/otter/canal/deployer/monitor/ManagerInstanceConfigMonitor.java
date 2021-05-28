package com.alibaba.otter.canal.deployer.monitor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;

/**
 * 基于manager配置的实现
 *
 * @author agapple 2019年8月26日 下午10:00:20
 * @since 1.1.4
 */
public class ManagerInstanceConfigMonitor extends AbstractCanalLifeCycle implements InstanceConfigMonitor, CanalLifeCycle {

    private static final Logger         logger               = LoggerFactory.getLogger(ManagerInstanceConfigMonitor.class);
    private long                        scanIntervalInSecond = 5;
    private InstanceAction              defaultAction        = null;
    private Map<String, InstanceAction> actions              = new MapMaker().makeMap();
    private Map<String, PlainCanal>     configs              = MigrateMap.makeComputingMap(destination -> new PlainCanal());
    private ScheduledExecutorService    executor             = Executors.newScheduledThreadPool(1,
                                                                 new NamedThreadFactory("canal-instance-scan"));

    private volatile boolean            isFirst              = true;
    private PlainCanalConfigClient      configClient;

    public void start() {
        super.start();
        executor.scheduleWithFixedDelay(() -> {
            try {
                scan();
                if (isFirst) {
                    isFirst = false;
                }
            } catch (Throwable e) {
                logger.error("scan failed", e);
            }
        }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
    }

    public void stop() {
        super.stop();
        executor.shutdownNow();
        actions.clear();
    }

    public void register(String destination, InstanceAction action) {
        if (action != null) {
            actions.put(destination, action);
        } else {
            actions.put(destination, defaultAction);
        }
    }

    public void unregister(String destination) {
        actions.remove(destination);
    }

    private void scan() {
        String instances = configClient.findInstances(null);
        if (instances == null) {
            return;
        }

        final List<String> is = Lists.newArrayList(StringUtils.split(instances, ','));
        List<String> start = Lists.newArrayList();
        List<String> stop = Lists.newArrayList();
        List<String> restart = Lists.newArrayList();
        for (String instance : is) {
            if (!configs.containsKey(instance)) {
                PlainCanal newPlainCanal = configClient.findInstance(instance, null);
                if (newPlainCanal != null) {
                    configs.put(instance, newPlainCanal);
                    start.add(instance);
                }
            } else {
                PlainCanal plainCanal = configs.get(instance);
                PlainCanal newPlainCanal = configClient.findInstance(instance, plainCanal.getMd5());
                if (newPlainCanal != null) {
                    // 配置有变化
                    restart.add(instance);
                    configs.put(instance, newPlainCanal);
                }
            }
        }

        configs.forEach((instance, plainCanal) -> {
            if (!is.contains(instance)) {
                stop.add(instance);
            }
        });

        stop.forEach(instance -> {
            notifyStop(instance);
        });

        restart.forEach(instance -> {
            notifyReload(instance);
        });

        start.forEach(instance -> {
            notifyStart(instance);
        });

    }

    private void notifyStart(String destination) {
        try {
            defaultAction.start(destination);
            actions.put(destination, defaultAction);
            // 启动成功后记录配置文件信息
        } catch (Throwable e) {
            logger.error(String.format("scan add found[%s] but start failed", destination), e);
        }
    }

    private void notifyStop(String destination) {
        InstanceAction action = actions.remove(destination);
        if (action != null) {
            try {
                action.stop(destination);
                configs.remove(destination);
            } catch (Throwable e) {
                logger.error(String.format("scan delete found[%s] but stop failed", destination), e);
                actions.put(destination, action);// 再重新加回去，下一次scan时再执行删除
            }
        }
    }

    private void notifyReload(String destination) {
        InstanceAction action = actions.get(destination);
        if (action != null) {
            try {
                action.reload(destination);
            } catch (Throwable e) {
                logger.error(String.format("scan reload found[%s] but reload failed", destination), e);
            }
        }
    }

    public void release(String destination) {
        InstanceAction action = actions.remove(destination);
        if (action != null) {
            try {
                configs.remove(destination);
            } catch (Throwable e) {
                logger.error(String.format("scan delete found[%s] but stop failed", destination), e);
                actions.put(destination, action);// 再重新加回去，下一次scan时再执行删除
            }
        }
    }

    public void setDefaultAction(InstanceAction defaultAction) {
        this.defaultAction = defaultAction;
    }

    public void setScanIntervalInSecond(long scanIntervalInSecond) {
        this.scanIntervalInSecond = scanIntervalInSecond;
    }

    public void setConfigClient(PlainCanalConfigClient configClient) {
        this.configClient = configClient;
    }

    public Map<String, InstanceAction> getActions() {
        return actions;
    }

}

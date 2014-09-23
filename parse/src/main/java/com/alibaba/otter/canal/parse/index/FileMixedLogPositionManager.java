package com.alibaba.otter.canal.parse.index;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

/**
 * 基于文件刷新的log position实现
 * 
 * <pre>
 * 策略：
 * 1. 先写内存，然后定时刷新数据到File
 * 2. 数据采取overwrite模式(只保留最后一次)
 * </pre>
 * 
 * @author jianghang 2013-4-15 下午09:40:48
 * @version 1.0.4
 */
public class FileMixedLogPositionManager extends MemoryLogPositionManager {

    private static final Logger      logger       = LoggerFactory.getLogger(FileMixedLogPositionManager.class);
    private static final Charset     charset      = Charset.forName("UTF-8");
    private File                     dataDir;
    private String                   dataFileName = "parse.dat";
    private Map<String, File>        dataFileCaches;
    private ScheduledExecutorService executor;
    @SuppressWarnings("serial")
    private final LogPosition        nullPosition = new LogPosition() {
                                                  };

    private long                     period       = 1000;                                                      // 单位ms
    private Set<String>              persistTasks;

    public void start() {
        super.start();

        Assert.notNull(dataDir);
        if (!dataDir.exists()) {
            try {
                FileUtils.forceMkdir(dataDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }

        if (!dataDir.canRead() || !dataDir.canWrite()) {
            throw new CanalMetaManagerException("dir[" + dataDir.getPath() + "] can not read/write");
        }

        dataFileCaches = new MapMaker().makeComputingMap(new Function<String, File>() {

            public File apply(String destination) {
                return getDataFile(destination);
            }
        });

        executor = Executors.newScheduledThreadPool(1);
        positions = new MapMaker().makeComputingMap(new Function<String, LogPosition>() {

            public LogPosition apply(String destination) {
                LogPosition logPosition = loadDataFromFile(dataFileCaches.get(destination));
                if (logPosition == null) {
                    return nullPosition;
                } else {
                    return logPosition;
                }
            }
        });

        persistTasks = Collections.synchronizedSet(new HashSet<String>());

        // 启动定时工作任务
        executor.scheduleAtFixedRate(new Runnable() {

            public void run() {
                List<String> tasks = new ArrayList<String>(persistTasks);
                for (String destination : tasks) {
                    try {
                        // 定时将内存中的最新值刷到file中，多次变更只刷一次
                        flushDataToFile(destination);
                        persistTasks.remove(destination);
                    } catch (Throwable e) {
                        // ignore
                        logger.error("period update" + destination + " curosr failed!", e);
                    }
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        super.stop();

        flushDataToFile();
        executor.shutdownNow();
        positions.clear();
    }

    public void persistLogPosition(String destination, LogPosition logPosition) {
        persistTasks.add(destination);// 添加到任务队列中进行触发
        super.persistLogPosition(destination, logPosition);
    }

    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPostion = super.getLatestIndexBy(destination);
        if (logPostion == nullPosition) {
            return null;
        } else {
            return logPostion;
        }
    }

    // ============================ helper method ======================

    private File getDataFile(String destination) {
        File destinationMetaDir = new File(dataDir, destination);
        if (!destinationMetaDir.exists()) {
            try {
                FileUtils.forceMkdir(destinationMetaDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }

        return new File(destinationMetaDir, dataFileName);
    }

    private void flushDataToFile() {
        for (String destination : positions.keySet()) {
            flushDataToFile(destination);
        }
    }

    private void flushDataToFile(String destination) {
        flushDataToFile(destination, dataFileCaches.get(destination));
    }

    private void flushDataToFile(String destination, File dataFile) {
        LogPosition position = positions.get(destination);
        if (position != null && position != nullPosition) {
            String json = JsonUtils.marshalToString(position);
            try {
                FileUtils.writeStringToFile(dataFile, json);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
    }

    private LogPosition loadDataFromFile(File dataFile) {
        try {
            if (!dataFile.exists()) {
                return null;
            }

            String json = FileUtils.readFileToString(dataFile, charset.name());
            return JsonUtils.unmarshalFromString(json, LogPosition.class);
        } catch (IOException e) {
            throw new CanalMetaManagerException(e);
        }
    }

    public void setDataDir(String dataDir) {
        this.dataDir = new File(dataDir);
    }

    public void setDataDir(File dataDir) {
        this.dataDir = dataDir;
    }

    public void setPeriod(long period) {
        this.period = period;
    }
}

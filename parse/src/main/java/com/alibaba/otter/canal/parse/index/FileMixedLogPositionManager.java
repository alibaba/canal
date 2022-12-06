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

import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.google.common.collect.MigrateMap;

/**
 * Created by yinxiu on 17/3/18. Email: marklin.hz@gmail.com 基于文件刷新的log
 * position实现
 *
 * <pre>
 * 策略：
 * 1. 先写内存，然后定时刷新数据到File
 * 2. 数据采取overwrite模式(只保留最后一次)
 * </pre>
 */
public class FileMixedLogPositionManager extends AbstractLogPositionManager {

    private final static Logger      logger       = LoggerFactory.getLogger(FileMixedLogPositionManager.class);
    private final static Charset     charset      = Charset.forName("UTF-8");

    private File                     dataDir;

    private Map<String, File>        dataFileCaches;

    private ScheduledExecutorService executorService;

    @SuppressWarnings("serial")
    private final LogPosition        nullPosition = new LogPosition() {
                                                  };

    private MemoryLogPositionManager memoryLogPositionManager;

    private long                     period;
    private Set<String>              persistTasks;

    public FileMixedLogPositionManager(File dataDir, long period, MemoryLogPositionManager memoryLogPositionManager){
        if (dataDir == null) {
            throw new NullPointerException("null dataDir");
        }
        if (period <= 0) {
            throw new IllegalArgumentException("period must be positive, given: " + period);
        }
        if (memoryLogPositionManager == null) {
            throw new NullPointerException("null memoryLogPositionManager");
        }
        this.dataDir = dataDir;
        this.period = period;
        this.memoryLogPositionManager = memoryLogPositionManager;

        this.dataFileCaches = MigrateMap.makeComputingMap(this::getDataFile);

        this.executorService = Executors.newScheduledThreadPool(1);
        this.persistTasks = Collections.synchronizedSet(new HashSet<>());
    }

    @Override
    public void start() {
        super.start();

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

        if (!memoryLogPositionManager.isStart()) {
            memoryLogPositionManager.start();
        }

        // 启动定时工作任务
        executorService.scheduleAtFixedRate(() -> {
            List<String> tasks = new ArrayList<>(persistTasks);
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
        }, period, period, TimeUnit.MILLISECONDS);

    }

    @Override
    public void stop() {
        super.stop();

        flushDataToFile();
        executorService.shutdown();
        memoryLogPositionManager.stop();
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = memoryLogPositionManager.getLatestIndexBy(destination);
        if (logPosition != null) {
            return logPosition;
        }
        logPosition = loadDataFromFile(dataFileCaches.get(destination));
        if (logPosition == null) {
            return nullPosition;
        }
        return logPosition;
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        persistTasks.add(destination);
        memoryLogPositionManager.persistLogPosition(destination, logPosition);
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

        String dataFileName = "parse.dat";
        return new File(destinationMetaDir, dataFileName);
    }

    private void flushDataToFile() {
        for (String destination : memoryLogPositionManager.destinations()) {
            flushDataToFile(destination);
        }
    }

    private void flushDataToFile(String destination) {
        flushDataToFile(destination, dataFileCaches.get(destination));
    }

    private void flushDataToFile(String destination, File dataFile) {
        LogPosition position = memoryLogPositionManager.getLatestIndexBy(destination);
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
}

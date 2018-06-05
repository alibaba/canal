package com.alibaba.canal.plumber.stage;

import com.alibaba.canal.plumber.schema.TransformService;
import com.alibaba.canal.plumber.tap.es.ElasticSearchBatchLoader;
import com.alibaba.canal.plumber.task.CanalClientTask;
import com.alibaba.canal.plumber.task.ElasticSearchStageTask;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 管道工的舞台...开始你的表演
 * @author dsqin
 * @date 2018/6/5
 */
public class StageController implements CanalLifeCycle {
    private static final Logger logger = LoggerFactory.getLogger(StageController.class);
    private ProcessQueue processQueue;
    private CanalConnector connector;
    private ElasticSearchBatchLoader batchLoader;
    private TransformService transformService;
    private boolean running = false;
    private boolean concurrent = false;
    private int thread;

    public void start() {
        logger.info("****stageController(主控程序)启动****");

        process();
        this.running = true;
    }

    public void stop() {
        this.running = false;
    }

    public boolean isStart() {
        return this.running;
    }

    private void process() {
        CanalClientTask canalClientTask = new CanalClientTask(this.connector, this.processQueue, this.transformService);

        Thread canalClientThread = new Thread(canalClientTask);

        logger.info("****CanalClient(数据获取)线程启动,线程名称:{}****", canalClientThread.getName());
        canalClientThread.start();
        if ((!this.concurrent) || ((this.concurrent) && (this.thread <= 0))) {
            this.thread = 1;
        }
        for (int i = 0; i < this.thread; i++) {
            ElasticSearchStageTask elasticSearchStageTask = new ElasticSearchStageTask(this.processQueue, this.batchLoader);

            Thread stageThread = new Thread(elasticSearchStageTask);

            logger.info("****ElasticSearchStage(数据写入)线程启动,线程编号:{}****", stageThread.getId());
            stageThread.start();
        }
    }

    public ProcessQueue getProcessQueue() {
        return this.processQueue;
    }

    public void setProcessQueue(ProcessQueue processQueue) {
        this.processQueue = processQueue;
    }

    public CanalConnector getConnector() {
        return this.connector;
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public ElasticSearchBatchLoader getBatchLoader() {
        return this.batchLoader;
    }

    public void setBatchLoader(ElasticSearchBatchLoader batchLoader) {
        this.batchLoader = batchLoader;
    }

    public TransformService getTransformService() {
        return this.transformService;
    }

    public void setTransformService(TransformService transformService) {
        this.transformService = transformService;
    }

    public boolean isConcurrent() {
        return this.concurrent;
    }

    public void setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
    }

    public int getThread() {
        return this.thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }
}
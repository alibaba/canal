package com.alibaba.canal.plumber.task;

import com.alibaba.canal.plumber.model.BatchData;
import com.alibaba.canal.plumber.model.BatchLoadContext;
import com.alibaba.canal.plumber.model.EventData;
import com.alibaba.canal.plumber.stage.ProcessQueue;
import com.alibaba.canal.plumber.tap.es.ElasticSearchBatchLoader;
import com.google.common.collect.Lists;
import java.io.PrintStream;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public class ElasticSearchStageTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchStageTask.class);
    private ProcessQueue processQueue;
    private ElasticSearchBatchLoader batchLoader;
    private int batchSize = 50;
    private int maxEmptySize = 10;

    public ElasticSearchStageTask(ProcessQueue processQueue, ElasticSearchBatchLoader batchLoader)
    {
        this.processQueue = processQueue;
        this.batchLoader = batchLoader;
    }

    public void run()
    {
        boolean running = true;
        while (running)
        {
            long s1 = System.currentTimeMillis();

            int emptyTimes = 0;
            List<EventData> eventDatas = Lists.newArrayList();
            for (int i = 0; i < this.batchSize; i++)
            {
                EventData eventData = this.processQueue.take();
                if (null == eventData)
                {
                    logger.info("****Plumber ElasticSearch������--������������������,sleep������,���������:{}****", Long.valueOf(Thread.currentThread().getId()));
                    if (this.maxEmptySize <= emptyTimes++) {
                        break;
                    }
                }
                else
                {
                    logger.info("****Plumber ElasticSearch������,���������������������������,������������:{},������������:{}****", Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()));

                    eventData.incrStage();

                    eventDatas.add(eventData);
                }
            }
            if (!eventDatas.isEmpty())
            {
                BatchData batchData = new BatchData(eventDatas);
                BatchLoadContext loadContext = this.batchLoader.load(batchData);

                System.out.println("*****002���" + (System.currentTimeMillis() - s1) + "ms size:" + eventDatas.size() + "****");
            }
        }
    }
}


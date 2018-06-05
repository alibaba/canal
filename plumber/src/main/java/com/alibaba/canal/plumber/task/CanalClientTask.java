package com.alibaba.canal.plumber.task;

import com.alibaba.canal.plumber.model.EventData;
import com.alibaba.canal.plumber.schema.TransformService;
import com.alibaba.canal.plumber.stage.ProcessQueue;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public class CanalClientTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(CanalClientTask.class);
    protected volatile boolean running = false;
    protected CanalConnector connector;
    protected ProcessQueue processQueue;
    protected TransformService transformService;

    public CanalClientTask(CanalConnector connector, ProcessQueue processQueue, TransformService transformService)
    {
        this.connector = connector;
        this.processQueue = processQueue;
        this.transformService = transformService;
    }

    public void run()
    {
        this.running = true;
        process();
    }

    private void process()
    {
        int batchSize = 100;
        this.connector.subscribe();
        while (this.running)
        {
            Message message = this.connector.getWithoutAck(batchSize);
            if (null != message)
            {
                long identity = message.getId();
                if ((null == message.getEntries()) || (message.getEntries().isEmpty()) || (-1L == identity))
                {
                    this.connector.ack(identity);
                }
                else
                {
                    logger.info("****CanalClient������������,������������:{},������������:{}****", Long.valueOf(identity), Integer.valueOf(message.getEntries().size()));

                    List<CanalEntry.Entry> entries = message.getEntries();
                    for (CanalEntry.Entry entry : entries)
                    {
                        EventData eventData = new EventData(entry);

                        logger.info("****Plumber������������������,���������������:{},������������:{},������������:{}*****", new Object[] { Long.valueOf(identity),
                                Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()) });

                        eventData.incrStage();
                        boolean success = this.transformService.transform(eventData);

                        logger.info("****Plumber transform������������,������������:{},������������:{},������:{}****", new Object[] { Long.valueOf(eventData.getSequenceId()),
                                Integer.valueOf(eventData.getStage()), Boolean.valueOf(success) });

                        eventData.incrStage();
                        if ((null != eventData) && (success))
                        {
                            boolean offerSucess = this.processQueue.offer(eventData);
                            logger.info("****Plumber������������������,������������:{},������������:{},������:{}****", new Object[] { Long.valueOf(eventData.getSequenceId()),
                                    Integer.valueOf(eventData.getStage()), Boolean.valueOf(offerSucess) });
                            eventData.incrStage();
                        }
                    }
                    this.connector.ack(identity);
                }
            }
        }
    }
}
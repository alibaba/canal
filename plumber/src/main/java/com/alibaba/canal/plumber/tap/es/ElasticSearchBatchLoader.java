package com.alibaba.canal.plumber.tap.es;

import com.alibaba.canal.plumber.model.BatchData;
import com.alibaba.canal.plumber.model.BatchLoadContext;
import com.alibaba.canal.plumber.model.EventData;
import com.alibaba.canal.plumber.tap.es.connection.ElasticSearchClient;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public class ElasticSearchBatchLoader {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBatchLoader.class);
    private ElasticSearchClient elasticSearchClient;

    public BatchLoadContext load(BatchData data)
    {
        List<EventData> rowBatch = data.getEventBatch();

        BatchLoadContext batchLoadContext = null;
        try
        {
            batchLoadContext = this.elasticSearchClient.execute(rowBatch);
        }
        catch (ExecutionException e)
        {
            logger.error("****es���������������������****", e);
            this.elasticSearchClient.reload();
        }
        catch (InterruptedException e)
        {
            logger.error("****es���������������������****", e);
            this.elasticSearchClient.reload();
        }
        return batchLoadContext;
    }

    public ElasticSearchClient getElasticSearchClient()
    {
        return this.elasticSearchClient;
    }

    public void setElasticSearchClient(ElasticSearchClient elasticSearchClient)
    {
        this.elasticSearchClient = elasticSearchClient;
    }
}

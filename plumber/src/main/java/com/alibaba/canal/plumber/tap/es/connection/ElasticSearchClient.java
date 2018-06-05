package com.alibaba.canal.plumber.tap.es.connection;

import com.alibaba.canal.plumber.model.BatchLoadContext;
import com.alibaba.canal.plumber.model.EventData;
import com.alibaba.canal.plumber.model.EventType;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public class ElasticSearchClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
    private static final String CLUSTER_NAME = "cluster.name";
    private TransportClient transportClient;
    private String connectURI;

    public ElasticSearchClient(String connectURI)
    {
        this.connectURI = connectURI;
    }

    public void createClient()
            throws URISyntaxException
    {
        logger.info("****���������elasticsearch���������,���������:{}****", this.connectURI);
        ElasticSearchConnectionParam elasticSearchConnectionParam = ElasticSearchURI.parse(this.connectURI);
        if (null == elasticSearchConnectionParam)
        {
            logger.error("****elasticsearch���������������������,���������:{}****", this.connectURI);
            throw new RuntimeException("���������������������");
        }
        Settings settings = Settings.builder().put("cluster.name", elasticSearchConnectionParam.getClusterName()).build();

        this.transportClient = new PreBuiltTransportClient(settings, new Class[0]);

        List<TransportAddress> transportAddresses = elasticSearchConnectionParam.getTransportAddresses();
        if ((null == transportAddresses) || (transportAddresses.isEmpty())) {
            throw new RuntimeException("������������������");
        }
        for (TransportAddress transportAddress : transportAddresses) {
            this.transportClient.addTransportAddress(transportAddress);
        }
        this.transportClient.listedNodes();
    }

    public void destory()
    {
        if (null != this.transportClient) {
            this.transportClient.close();
        }
    }

    public void reload()
    {
        destory();
        try
        {
            createClient();
        }
        catch (URISyntaxException localURISyntaxException) {}
    }

    public BatchLoadContext execute(List<EventData> eventDataList)
            throws ExecutionException, InterruptedException
    {
        BulkRequestBuilder bulkRequestBuilder = this.transportClient.prepareBulk();
        if ((null == eventDataList) || (eventDataList.isEmpty())) {
            return new BatchLoadContext(true);
        }
        for (EventData eventData : eventDataList)
        {
            logger.info("****Plumber ElasticSearch������������,������������:{},������������:{}****", Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()));
            Map<String, String> headers = eventData.getHeaders();
            if ((null == headers) || (headers.isEmpty()))
            {
                logger.info("****Plumber ElasticSearch������������������,header���������������,������������:{},������������:{}****",
                        Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()));
            }
            else
            {
                String index = (String)headers.get("index");
                String type = (String)headers.get("type");
                String pk = StringUtils.defaultIfBlank((String)headers.get("pk"), "_id");
                String embed = (String)headers.get("embed");

                String body = String.valueOf(eventData.getBody());
                JSONObject jsonObject = JSON.parseObject(body);
                String id = jsonObject.getString(pk);

                jsonObject.remove(pk);

                EventType eventType = eventData.getEventType();
                if ((eventType.isInsert()) || (eventType.isUpdate()))
                {
                    UpdateRequest updateRequest = upsert(index, type, id, jsonObject, embed);
                    if (null != updateRequest) {
                        bulkRequestBuilder.add(updateRequest);
                    }
                }
                else if (eventType.isDelete())
                {
                    DeleteRequest deleteRequest = delete(index, type, id);
                    bulkRequestBuilder.add(deleteRequest);
                }
                eventData.incrStage();

                logger.info("****Plumber ElasticSearch������������������,������������:{},������������:{}****",
                        Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()));
            }
        }
        Object futures = bulkRequestBuilder.execute();

        List processedList = Lists.newArrayList();
        List failureList = Lists.newArrayList();

        BulkResponse bulkResponse = (BulkResponse)((ListenableActionFuture)futures).get();

        BatchLoadContext batchLoadContext = new BatchLoadContext();
        batchLoadContext.setProcessedDatas(processedList);
        batchLoadContext.setFailedDatas(failureList);

        return batchLoadContext;
    }

    private UpdateRequest upsert(String index, String type, String id, JSONObject jsonObject, String embed)
    {
        IndexRequest indexRequest = new IndexRequest(index, type, id);

        XContentBuilder xContentBuilder = null;
        try
        {
            if (StringUtils.isNotBlank(embed)) {
                xContentBuilder = XContentFactory.jsonBuilder().startObject().field(embed, jsonObject.getJSONObject(embed)).endObject();
            } else {
                xContentBuilder = XContentFactory.jsonBuilder().map(jsonObject);
            }
        }
        catch (IOException ex)
        {
            ex.printStackTrace();

            return null;
        }
        indexRequest.source(xContentBuilder);

        return new UpdateRequest(index, type, id)
                .doc(xContentBuilder)
                .upsert(indexRequest);
    }

    public DeleteRequest delete(String index, String type, String id)
    {
        return new DeleteRequest(index, type, id);
    }
}

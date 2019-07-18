package com.alibaba.otter.canal.client.adapter.es.test;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class ESRestTest {

    private RestHighLevelClient restHighLevelClient;

    @Before
    public void init() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "elastic"));
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("192.168.81.48", 9200))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        restHighLevelClient = new RestHighLevelClient(clientBuilder);
    }

    @Test
    public void test01() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("_id", "1")).size(10000);
        SearchRequest searchRequest = new SearchRequest("mytest1_user");
        searchRequest.source(builder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        SearchResponse response = restHighLevelClient.prepareSearch("test")
//            .setTypes("osm")
//            .setQuery(QueryBuilders.termQuery("_id", "1"))
//            .setSize(10000)
//            .get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsMap().get("name"));
        }
    }

    @Test
    public void test02() throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices("mytest1_user");
        GetMappingsResponse response = restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
        Map<String, MappingMetaData> mappings = response.mappings();
        System.out.println(mappings);
    }

//    @Test
//    public void test02() {
//        Map<String, Object> esFieldData = new LinkedHashMap<>();
//        esFieldData.put("userId", 2L);
//        esFieldData.put("eventId", 4L);
//        esFieldData.put("eventName", "网络异常");
//        esFieldData.put("description", "第四个事件信息");
//
//        BulkRequest bulkRequest = new BulkRequest();
//        bulkRequest.add(new IndexRequest())
//
//        Map<String, Object> relations = new LinkedHashMap<>();
//        esFieldData.put("user_event", relations);
//        relations.put("name", "event");
//        relations.put("parent", "2");
//
//        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
//        bulkRequestBuilder
//            .add(transportClient.prepareIndex("test", "osm", "2_4").setRouting("2").setSource(esFieldData));
//        commit(bulkRequestBuilder);
//    }
//
//    @Test
//    public void test03() {
//        Map<String, Object> esFieldData = new LinkedHashMap<>();
//        esFieldData.put("userId", 2L);
//        esFieldData.put("eventName", "网络异常1");
//
//        Map<String, Object> relations = new LinkedHashMap<>();
//        esFieldData.put("user_event", relations);
//        relations.put("name", "event");
//        relations.put("parent", "2");
//
//        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
//        bulkRequestBuilder.add(transportClient.prepareUpdate("test", "osm", "2_4").setRouting("2").setDoc(esFieldData));
//        commit(bulkRequestBuilder);
//    }
//
//    @Test
//    public void test04() {
//        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
//        bulkRequestBuilder.add(transportClient.prepareDelete("test", "osm", "2_4"));
//        commit(bulkRequestBuilder);
//    }
//
//    private void commit(BulkRequestBuilder bulkRequestBuilder) {
//        if (bulkRequestBuilder.numberOfActions() > 0) {
//            BulkResponse response = bulkRequestBuilder.execute().actionGet();
//            if (response.hasFailures()) {
//                for (BulkItemResponse itemResponse : response.getItems()) {
//                    if (!itemResponse.isFailed()) {
//                        continue;
//                    }
//
//                    if (itemResponse.getFailure().getStatus() == RestStatus.NOT_FOUND) {
//                        System.out.println(itemResponse.getFailureMessage());
//                    } else {
//                        System.out.println("ES bulk commit error" + itemResponse.getFailureMessage());
//                    }
//                }
//            }
//        }
//    }

    @After
    public void after() throws IOException {
        restHighLevelClient.close();
    }
}

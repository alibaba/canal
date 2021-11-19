package com.alibaba.otter.canal.client.adapter.es6x.support;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientExt;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest;

/**
 * ES 连接器, Transport Rest 两种方式
 *
 * @author rewerma 2019-08-01
 * @version 1.0.0
 */
public class ESConnection {

    private static final Logger logger = LoggerFactory.getLogger(ESConnection.class);

    public enum ESClientMode {
        TRANSPORT, REST
    }

    private ESClientMode        mode;

    private TransportClient     transportClient;

    private RestHighLevelClient restHighLevelClient;

    public ESConnection(String[] hosts, Map<String, String> properties, ESClientMode mode) throws UnknownHostException{
        this.mode = mode;
        if (mode == ESClientMode.TRANSPORT) {
            Settings.Builder settingBuilder = Settings.builder();
            settingBuilder.put("cluster.name", properties.get("cluster.name"));
            Settings settings = settingBuilder.build();
            transportClient = new PreBuiltTransportClient(settings);
            for (String host : hosts) {
                int i = host.indexOf(":");
                transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host.substring(0, i)),
                    Integer.parseInt(host.substring(i + 1))));
            }
        } else {
            HttpHost[] httpHosts = Arrays.stream(hosts).map(this::createHttpHost).toArray(HttpHost[]::new);
            RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
            String nameAndPwd = properties.get("security.auth");
            if (StringUtils.isNotEmpty(nameAndPwd) && nameAndPwd.contains(":")) {
                String[] nameAndPwdArr = nameAndPwd.split(":");
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(nameAndPwdArr[0],
                    nameAndPwdArr[1]));
                restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }
    }

    public void close() {
        if (mode == ESClientMode.TRANSPORT) {
            transportClient.close();
        } else {
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public MappingMetaData getMapping(String index, String type) {
        MappingMetaData mappingMetaData = null;
        if (mode == ESClientMode.TRANSPORT) {
            ImmutableOpenMap<String, MappingMetaData> mappings;
            try {
                mappings = transportClient.admin()
                    .cluster()
                    .prepareState()
                    .execute()
                    .actionGet()
                    .getState()
                    .getMetaData()
                    .getIndices()
                    .get(index)
                    .getMappings();
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + index);
            }
            mappingMetaData = mappings.get(type);

        } else {
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings;
            try {
                GetMappingsRequest request = new GetMappingsRequest();
                request.indices(index);
                GetMappingsResponse response;
                // try {
                // response = restHighLevelClient
                // .indices()
                // .getMapping(request, RequestOptions.DEFAULT);
                // // 6.4以下版本直接使用该接口会报错
                // } catch (Exception e) {
                // logger.warn("Low ElasticSearch version for getMapping");
                response = RestHighLevelClientExt.getMapping(restHighLevelClient, request, RequestOptions.DEFAULT);
                // }

                mappings = response.mappings();
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + index);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
            mappingMetaData = mappings.get(index).get(type);
        }
        return mappingMetaData;
    }

    public class ES6xIndexRequest implements ESBulkRequest.ESIndexRequest {

        private IndexRequestBuilder indexRequestBuilder;

        private IndexRequest        indexRequest;

        public ES6xIndexRequest(String index, String type, String id){
            if (mode == ESClientMode.TRANSPORT) {
                indexRequestBuilder = transportClient.prepareIndex(index, type, id);
            } else {
                indexRequest = new IndexRequest(index, type, id);
            }
        }

        public ES6xIndexRequest setSource(Map<String, ?> source) {
            if (mode == ESClientMode.TRANSPORT) {
                indexRequestBuilder.setSource(source);
            } else {
                indexRequest.source(source);
            }
            return this;
        }

        public ES6xIndexRequest setRouting(String routing) {
            if (mode == ESClientMode.TRANSPORT) {
                indexRequestBuilder.setRouting(routing);
            } else {
                indexRequest.routing(routing);
            }
            return this;
        }

        public IndexRequestBuilder getIndexRequestBuilder() {
            return indexRequestBuilder;
        }

        public void setIndexRequestBuilder(IndexRequestBuilder indexRequestBuilder) {
            this.indexRequestBuilder = indexRequestBuilder;
        }

        public IndexRequest getIndexRequest() {
            return indexRequest;
        }

        public void setIndexRequest(IndexRequest indexRequest) {
            this.indexRequest = indexRequest;
        }
    }

    public class ES6xUpdateRequest implements ESBulkRequest.ESUpdateRequest {

        private UpdateRequestBuilder updateRequestBuilder;

        private UpdateRequest        updateRequest;

        public ES6xUpdateRequest(String index, String type, String id){
            if (mode == ESClientMode.TRANSPORT) {
                updateRequestBuilder = transportClient.prepareUpdate(index, type, id);
            } else {
                updateRequest = new UpdateRequest(index, type, id);
            }
        }

        public ES6xUpdateRequest setDoc(Map source) {
            if (mode == ESClientMode.TRANSPORT) {
                updateRequestBuilder.setDoc(source);
            } else {
                updateRequest.doc(source);
            }
            return this;
        }

        public ES6xUpdateRequest setDocAsUpsert(boolean shouldUpsertDoc) {
            if (mode == ESClientMode.TRANSPORT) {
                updateRequestBuilder.setDocAsUpsert(shouldUpsertDoc);
            } else {
                updateRequest.docAsUpsert(shouldUpsertDoc);
            }
            return this;
        }

        public ES6xUpdateRequest setRouting(String routing) {
            if (mode == ESClientMode.TRANSPORT) {
                updateRequestBuilder.setRouting(routing);
            } else {
                updateRequest.routing(routing);
            }
            return this;
        }

        public UpdateRequestBuilder getUpdateRequestBuilder() {
            return updateRequestBuilder;
        }

        public void setUpdateRequestBuilder(UpdateRequestBuilder updateRequestBuilder) {
            this.updateRequestBuilder = updateRequestBuilder;
        }

        public UpdateRequest getUpdateRequest() {
            return updateRequest;
        }

        public void setUpdateRequest(UpdateRequest updateRequest) {
            this.updateRequest = updateRequest;
        }
    }

    public class ES6xDeleteRequest implements ESBulkRequest.ESDeleteRequest {

        private DeleteRequestBuilder deleteRequestBuilder;

        private DeleteRequest        deleteRequest;

        public ES6xDeleteRequest(String index, String type, String id){
            if (mode == ESClientMode.TRANSPORT) {
                deleteRequestBuilder = transportClient.prepareDelete(index, type, id);
            } else {
                deleteRequest = new DeleteRequest(index, type, id);
            }
        }

        public DeleteRequestBuilder getDeleteRequestBuilder() {
            return deleteRequestBuilder;
        }

        public void setDeleteRequestBuilder(DeleteRequestBuilder deleteRequestBuilder) {
            this.deleteRequestBuilder = deleteRequestBuilder;
        }

        public DeleteRequest getDeleteRequest() {
            return deleteRequest;
        }

        public void setDeleteRequest(DeleteRequest deleteRequest) {
            this.deleteRequest = deleteRequest;
        }
    }

    public class ESSearchRequest {

        private SearchRequestBuilder searchRequestBuilder;

        private SearchRequest        searchRequest;

        private SearchSourceBuilder  sourceBuilder;

        public ESSearchRequest(String index, String... types){
            if (mode == ESClientMode.TRANSPORT) {
                searchRequestBuilder = transportClient.prepareSearch(index).setTypes(types);
            } else {
                searchRequest = new SearchRequest(index).types(types);
                sourceBuilder = new SearchSourceBuilder();
            }
        }

        public ESSearchRequest setQuery(QueryBuilder queryBuilder) {
            if (mode == ESClientMode.TRANSPORT) {
                searchRequestBuilder.setQuery(queryBuilder);
            } else {
                sourceBuilder.query(queryBuilder);
            }
            return this;
        }

        public ESSearchRequest size(int size) {
            if (mode == ESClientMode.TRANSPORT) {
                searchRequestBuilder.setSize(size);
            } else {
                sourceBuilder.size(size);
            }
            return this;
        }

        public SearchResponse getResponse() {
            if (mode == ESClientMode.TRANSPORT) {
                return searchRequestBuilder.get();
            } else {
                searchRequest.source(sourceBuilder);
                try {
                    return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public SearchRequestBuilder getSearchRequestBuilder() {
            return searchRequestBuilder;
        }

        public void setSearchRequestBuilder(SearchRequestBuilder searchRequestBuilder) {
            this.searchRequestBuilder = searchRequestBuilder;
        }

        public SearchRequest getSearchRequest() {
            return searchRequest;
        }

        public void setSearchRequest(SearchRequest searchRequest) {
            this.searchRequest = searchRequest;
        }
    }

    public class ES6xBulkRequest implements ESBulkRequest {

        private BulkRequestBuilder bulkRequestBuilder;

        private BulkRequest        bulkRequest;

        public ES6xBulkRequest(){
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder = transportClient.prepareBulk();
            } else {
                bulkRequest = new BulkRequest();
            }
        }

        public void resetBulk() {
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder = transportClient.prepareBulk();
            } else {
                bulkRequest = new BulkRequest();
            }
        }

        public ES6xBulkRequest add(ESIndexRequest esIndexRequest) {
            ES6xIndexRequest eir = (ES6xIndexRequest) esIndexRequest;
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder.add(eir.indexRequestBuilder);
            } else {
                bulkRequest.add(eir.indexRequest);
            }
            return this;
        }

        public ES6xBulkRequest add(ESUpdateRequest esUpdateRequest) {
            ES6xUpdateRequest eur = (ES6xUpdateRequest) esUpdateRequest;
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder.add(eur.updateRequestBuilder);
            } else {
                bulkRequest.add(eur.updateRequest);
            }
            return this;
        }

        public ES6xBulkRequest add(ESDeleteRequest esDeleteRequest) {
            ES6xDeleteRequest edr = (ES6xDeleteRequest) esDeleteRequest;
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder.add(edr.deleteRequestBuilder);
            } else {
                bulkRequest.add(edr.deleteRequest);
            }
            return this;
        }

        public int numberOfActions() {
            if (mode == ESClientMode.TRANSPORT) {
                return bulkRequestBuilder.numberOfActions();
            } else {
                return bulkRequest.numberOfActions();
            }
        }

        @SuppressWarnings("deprecation")
        public ESBulkResponse bulk() {
            if (mode == ESClientMode.TRANSPORT) {
                BulkResponse responses = bulkRequestBuilder.execute().actionGet();
                return new ES6xBulkResponse(responses);
            } else {
                try {
                    BulkResponse responses = restHighLevelClient.bulk(bulkRequest);
                    return new ES6xBulkResponse(responses);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public BulkRequestBuilder getBulkRequestBuilder() {
            return bulkRequestBuilder;
        }

        public void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
            this.bulkRequestBuilder = bulkRequestBuilder;
        }

        public BulkRequest getBulkRequest() {
            return bulkRequest;
        }

        public void setBulkRequest(BulkRequest bulkRequest) {
            this.bulkRequest = bulkRequest;
        }
    }

    public static class ES6xBulkResponse implements ESBulkRequest.ESBulkResponse {

        private BulkResponse bulkResponse;

        public ES6xBulkResponse(BulkResponse bulkResponse){
            this.bulkResponse = bulkResponse;
        }

        @Override
        public boolean hasFailures() {
            return bulkResponse.hasFailures();
        }

        @Override
        public void processFailBulkResponse(String errorMsg) {
            for (BulkItemResponse itemResponse : bulkResponse.getItems()) {
                if (!itemResponse.isFailed()) {
                    continue;
                }

                if (itemResponse.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                    logger.error(itemResponse.getFailureMessage());
                } else {
                    throw new RuntimeException(errorMsg + itemResponse.getFailureMessage());
                }
            }
        }
    }

    // ------ get/set ------
    public ESClientMode getMode() {
        return mode;
    }

    public void setMode(ESClientMode mode) {
        this.mode = mode;
    }

    public TransportClient getTransportClient() {
        return transportClient;
    }

    public void setTransportClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }

    public void setRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    private HttpHost createHttpHost(String uriStr) {
        URI uri = URI.create(uriStr);
        if (!org.springframework.util.StringUtils.hasLength(uri.getUserInfo())) {
            return HttpHost.create(uri.toString());
        }
        try {
            return HttpHost.create(new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(),
                                           uri.getQuery(), uri.getFragment()).toString());
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }
}

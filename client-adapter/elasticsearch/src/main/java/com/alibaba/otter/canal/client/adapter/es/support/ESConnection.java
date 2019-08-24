package com.alibaba.otter.canal.client.adapter.es.support;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
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
import org.elasticsearch.client.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            properties.forEach(settingBuilder::put);
            Settings settings = settingBuilder.build();
            transportClient = new PreBuiltTransportClient(settings);
            for (String host : hosts) {
                int i = host.indexOf(":");
                transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host.substring(0, i)),
                    Integer.parseInt(host.substring(i + 1))));
            }
        } else {
            HttpHost[] httpHosts = new HttpHost[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                String host = hosts[i];
                int j = host.indexOf(":");
                HttpHost httpHost = new HttpHost(InetAddress.getByName(host.substring(0, j)),
                    Integer.parseInt(host.substring(j + 1)));
                httpHosts[i] = httpHost;
            }
            RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
            String nameAndPwd = properties.get("security.auth");
            if (StringUtils.isNotEmpty(nameAndPwd) && nameAndPwd.contains(":")) {
                String[] nameAndPwdArr = nameAndPwd.split(":");
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(nameAndPwdArr[0], nameAndPwdArr[1]));
                restClientBuilder.setHttpClientConfigCallback(
                    httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
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

    public class ESIndexRequest {

        private IndexRequestBuilder indexRequestBuilder;

        private IndexRequest        indexRequest;

        public ESIndexRequest(String index, String type, String id){
            if (mode == ESClientMode.TRANSPORT) {
                indexRequestBuilder = transportClient.prepareIndex(index, type, id);
            } else {
                indexRequest = new IndexRequest(index, type, id);
            }
        }

        public ESIndexRequest setSource(Map<String, ?> source) {
            if (mode == ESClientMode.TRANSPORT) {
                indexRequestBuilder.setSource(source);
            } else {
                indexRequest.source(source);
            }
            return this;
        }

        public ESIndexRequest setRouting(String routing) {
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

    public class ESUpdateRequest {

        private UpdateRequestBuilder updateRequestBuilder;

        private UpdateRequest        updateRequest;

        public ESUpdateRequest(String index, String type, String id){
            if (mode == ESClientMode.TRANSPORT) {
                updateRequestBuilder = transportClient.prepareUpdate(index, type, id);
            } else {
                updateRequest = new UpdateRequest(index, type, id);
            }
        }

        public ESUpdateRequest setDoc(Map source) {
            if (mode == ESClientMode.TRANSPORT) {
                updateRequestBuilder.setDoc(source);
            } else {
                updateRequest.doc(source);
            }
            return this;
        }

        public ESUpdateRequest setDocAsUpsert(boolean shouldUpsertDoc) {
            if (mode == ESClientMode.TRANSPORT) {
                updateRequestBuilder.setDocAsUpsert(shouldUpsertDoc);
            } else {
                updateRequest.docAsUpsert(shouldUpsertDoc);
            }
            return this;
        }

        public ESUpdateRequest setRouting(String routing) {
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

    public class ESDeleteRequest {

        private DeleteRequestBuilder deleteRequestBuilder;

        private DeleteRequest        deleteRequest;

        public ESDeleteRequest(String index, String type, String id){
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

    public class ESBulkRequest {

        private BulkRequestBuilder bulkRequestBuilder;

        private BulkRequest        bulkRequest;

        public ESBulkRequest(){
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

        public ESBulkRequest add(ESIndexRequest esIndexRequest) {
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder.add(esIndexRequest.indexRequestBuilder);
            } else {
                bulkRequest.add(esIndexRequest.indexRequest);
            }
            return this;
        }

        public ESBulkRequest add(ESUpdateRequest esUpdateRequest) {
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder.add(esUpdateRequest.updateRequestBuilder);
            } else {
                bulkRequest.add(esUpdateRequest.updateRequest);
            }
            return this;
        }

        public ESBulkRequest add(ESDeleteRequest esDeleteRequest) {
            if (mode == ESClientMode.TRANSPORT) {
                bulkRequestBuilder.add(esDeleteRequest.deleteRequestBuilder);
            } else {
                bulkRequest.add(esDeleteRequest.deleteRequest);
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

        public BulkResponse bulk() {
            if (mode == ESClientMode.TRANSPORT) {
                return bulkRequestBuilder.execute().actionGet();
            } else {
                try {
                    return restHighLevelClient.bulk(bulkRequest);
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
}

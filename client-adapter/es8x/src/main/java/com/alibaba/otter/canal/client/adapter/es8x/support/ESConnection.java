package com.alibaba.otter.canal.client.adapter.es8x.support;

import java.io.IOException;
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
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest;

/**
 * ES 连接器, 只支持 Rest 方式
 *
 * @author ymz 2023-03-02
 * @version 1.0.0
 */
public class ESConnection {

    private static final Logger logger = LoggerFactory.getLogger(ESConnection.class);

    private RestHighLevelClient restHighLevelClient;

    public ESConnection(String[] hosts, Map<String, String> properties) throws UnknownHostException{
        HttpHost[] httpHosts = Arrays.stream(hosts).map(this::createHttpHost).toArray(HttpHost[]::new);
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
        restHighLevelClient = new RestHighLevelClientBuilder(restClientBuilder.build()).setApiCompatibilityMode(true)
            .build();
    }

    public void close() {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MappingMetadata getMapping(String index) {
        MappingMetadata mappingMetaData = null;

        Map<String, MappingMetadata> mappings;
        try {
            GetMappingsRequest request = new GetMappingsRequest();
            request.indices(index);
            GetMappingsResponse response = restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);

            mappings = response.mappings();
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Not found the mapping info of index: " + index);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
        mappingMetaData = mappings.get(index);

        return mappingMetaData;
    }

    public class ES8xIndexRequest implements ESBulkRequest.ESIndexRequest {

        private IndexRequestBuilder indexRequestBuilder;

        private IndexRequest        indexRequest;

        public ES8xIndexRequest(String index, String id){
            indexRequest = new IndexRequest(index);
            indexRequest.id(id);

        }

        public ES8xIndexRequest setSource(Map<String, ?> source) {

            indexRequest.source(source);

            return this;
        }

        public ES8xIndexRequest setRouting(String routing) {

            indexRequest.routing(routing);

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

    public class ES8xUpdateRequest implements ESBulkRequest.ESUpdateRequest {

        private UpdateRequestBuilder updateRequestBuilder;

        private UpdateRequest        updateRequest;

        public ES8xUpdateRequest(String index, String id){

            updateRequest = new UpdateRequest(index, id);
        }

        public ES8xUpdateRequest setDoc(Map source) {

            updateRequest.doc(source);

            return this;
        }

        public ES8xUpdateRequest setDocAsUpsert(boolean shouldUpsertDoc) {

            updateRequest.docAsUpsert(shouldUpsertDoc);

            return this;
        }

        public ES8xUpdateRequest setRouting(String routing) {

            updateRequest.routing(routing);

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

    public class ES8xDeleteRequest implements ESBulkRequest.ESDeleteRequest {

        private DeleteRequestBuilder deleteRequestBuilder;

        private DeleteRequest        deleteRequest;

        public ES8xDeleteRequest(String index, String id){

            deleteRequest = new DeleteRequest(index, id);

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

        public ESSearchRequest(String index){

            searchRequest = new SearchRequest(index);
            sourceBuilder = new SearchSourceBuilder();

        }

        public ESSearchRequest setQuery(QueryBuilder queryBuilder) {

            sourceBuilder.query(queryBuilder);

            return this;
        }

        public ESSearchRequest size(int size) {

            sourceBuilder.size(size);

            return this;
        }

        public SearchResponse getResponse() {

            searchRequest.source(sourceBuilder);
            try {
                return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new RuntimeException(e);
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

    public class ES8xBulkRequest implements ESBulkRequest {

        private BulkRequestBuilder bulkRequestBuilder;

        private BulkRequest        bulkRequest;

        public ES8xBulkRequest(){

            bulkRequest = new BulkRequest();

        }

        public void resetBulk() {

            bulkRequest = new BulkRequest();

        }

        public ES8xBulkRequest add(ESIndexRequest esIndexRequest) {
            ES8xIndexRequest eir = (ES8xIndexRequest) esIndexRequest;

            bulkRequest.add(eir.indexRequest);

            return this;
        }

        public ES8xBulkRequest add(ESUpdateRequest esUpdateRequest) {
            ES8xUpdateRequest eur = (ES8xUpdateRequest) esUpdateRequest;

            bulkRequest.add(eur.updateRequest);

            return this;
        }

        public ES8xBulkRequest add(ESDeleteRequest esDeleteRequest) {
            ES8xDeleteRequest edr = (ES8xDeleteRequest) esDeleteRequest;

            bulkRequest.add(edr.deleteRequest);

            return this;
        }

        public int numberOfActions() {
            return bulkRequest.numberOfActions();
        }

        public ESBulkResponse bulk() {
            try {
                BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                return new ES8xBulkResponse(responses);
            } catch (IOException e) {
                throw new RuntimeException(e);
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

    public static class ES8xBulkResponse implements ESBulkRequest.ESBulkResponse {

        private BulkResponse bulkResponse;

        public ES8xBulkResponse(BulkResponse bulkResponse){
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
            return HttpHost.create(new URI(uri
                .getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment())
                    .toString());
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }
}

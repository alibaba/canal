package com.alibaba.otter.canal.client.adapter.es.core.support;

import java.util.Map;

public interface ESBulkRequest {

    void resetBulk();

    ESBulkRequest add(ESIndexRequest esIndexRequest);

    ESBulkRequest add(ESUpdateRequest esUpdateRequest);

    ESBulkRequest add(ESDeleteRequest esDeleteRequest);

    int numberOfActions();

    ESBulkResponse bulk();

    interface ESIndexRequest {

        ESIndexRequest setSource(Map<String, ?> source);

        ESIndexRequest setRouting(String routing);
    }

    interface ESUpdateRequest {

        ESUpdateRequest setDoc(Map source);

        ESUpdateRequest setDocAsUpsert(boolean shouldUpsertDoc);

        ESUpdateRequest setRouting(String routing);
    }

    interface ESDeleteRequest {
    }

    interface ESBulkResponse {
        boolean hasFailures();

        void processFailBulkResponse(String errorMsg);
    }
}

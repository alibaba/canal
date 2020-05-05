package com.alibaba.otter.canal.client.adapter.es.core.support;

import java.util.Map;
import java.util.function.Function;

public interface ESBulkRequest {

    void resetBulk();

    default ESBulkRequest add(IESRequest esRequest) {
        return esRequest.add(this);
    }

    /**
     * @param esRequest es请求对象
     * @param commitBatchSize 批次提交大小（单位为字节）
     * @param ifGtCommitBatchSize 回调函数（入参：追加内容字节长度。当批中超出 批次提交大小(commitBatchSize) 限制, 且回调函数返回true就可以继续添加, 否则就抛弃这一条）
     * @return 是否添加成功
     */
    default boolean add(IESRequest esRequest, int commitBatchSize, Function<Long, Boolean> ifGtCommitBatchSize) {
        return esRequest.add(this, commitBatchSize, ifGtCommitBatchSize);
    }

    int numberOfActions();

    /**
     * 批次大小（单位为字节）
     * @return
     */
    long estimatedSizeInBytes();

    ESBulkResponse bulk();

    interface IESRequest {

        ESBulkRequest add(ESBulkRequest esBulkRequest);

        /**
         * @param esBulkRequest es请求批次
         * @param commitBatchSize 批次提交大小（单位为字节）
         * @param ifGtCommitBatchSize 回调函数（入参：追加内容字节长度。当批中超出 批次提交大小(commitBatchSize) 限制, 且回调函数返回true就可以继续添加, 否则就抛弃这一条）
         * @return 是否添加成功
         */
        boolean add(ESBulkRequest esBulkRequest, int commitBatchSize, Function<Long, Boolean> ifGtCommitBatchSize);
    }

    interface ESIndexRequest extends IESRequest {

        ESIndexRequest setSource(Map<String, ?> source);

        ESIndexRequest setRouting(String routing);
    }

    interface ESUpdateRequest extends IESRequest {

        ESUpdateRequest setDoc(Map source);

        ESUpdateRequest setDocAsUpsert(boolean shouldUpsertDoc);

        ESUpdateRequest setRouting(String routing);
    }

    interface ESDeleteRequest extends IESRequest {
    }

    interface ESBulkResponse {
        boolean hasFailures();

        void processFailBulkResponse(String errorMsg);
    }
}

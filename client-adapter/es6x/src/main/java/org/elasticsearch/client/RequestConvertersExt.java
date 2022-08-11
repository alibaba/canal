package org.elasticsearch.client;

import java.io.IOException;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.common.Strings;

/**
 * RequestConverters扩展
 *
 * @author rewerma 2019-08-01
 * @version 1.0.0
 */
public class RequestConvertersExt {

    /**
     * 修改 getMappings 去掉request参数
     *
     * @param getMappingsRequest
     * @return
     * @throws IOException
     */
    static Request getMappings(GetMappingsRequest getMappingsRequest) throws IOException {
        String[] indices = getMappingsRequest.indices() == null ? Strings.EMPTY_ARRAY : getMappingsRequest.indices();
        String[] types = getMappingsRequest.types() == null ? Strings.EMPTY_ARRAY : getMappingsRequest.types();

        return new Request(HttpGet.METHOD_NAME, RequestConverters.endpoint(indices, "_mapping", types));
    }
}

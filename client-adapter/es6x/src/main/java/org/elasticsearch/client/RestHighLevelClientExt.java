package org.elasticsearch.client;

import static java.util.Collections.emptySet;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;

/**
 * RestHighLevelClient扩展
 *
 * @author rewerma 2019-08-01
 * @version 1.0.0
 */
public class RestHighLevelClientExt {

    public static GetMappingsResponse getMapping(RestHighLevelClient restHighLevelClient,
                                                 GetMappingsRequest getMappingsRequest,
                                                 RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(getMappingsRequest,
            RequestConvertersExt::getMappings,
            options,
            GetMappingsResponse::fromXContent,
            emptySet());

    }

}

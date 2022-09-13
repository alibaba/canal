/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.row.sink;

import org.apache.flink.types.RowKind;

/**
 * StarRocks sink operator.
 */
public enum StarRocksSinkOP {
    UPSERT, DELETE;

    public static final String COLUMN_KEY = "__op";

    static StarRocksSinkOP parse(RowKind kind) {
        if (RowKind.INSERT.equals(kind) || RowKind.UPDATE_AFTER.equals(kind)) {
            return UPSERT;
        }
        if (RowKind.DELETE.equals(kind) || RowKind.UPDATE_BEFORE.equals(kind)) {
            return DELETE;
        }
        throw new RuntimeException("Unsupported row kind.");
    }
}

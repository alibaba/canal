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

package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOptions;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class StarRocksSinkManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private final StarRocksJdbcConnectionProvider jdbcConnProvider;
    private final StarRocksQueryVisitor starrocksQueryVisitor;
    private StarRocksStreamLoadVisitor starrocksStreamLoadVisitor;
    private final StarRocksSinkOptions sinkOptions;

    public StarRocksSinkManager(StarRocksSinkOptions sinkOptions, TableSchema flinkSchema) {
        this.sinkOptions = sinkOptions;
        StarRocksJdbcConnectionOptions jdbcOptions = new StarRocksJdbcConnectionOptions(sinkOptions.getJdbcUrl(), sinkOptions.getUsername(), sinkOptions.getPassword());
        this.jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        this.starrocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnProvider, sinkOptions.getDatabaseName(), sinkOptions.getTableName());

        init(flinkSchema);
    }


    protected void init(TableSchema schema) {
        validateTableStructure(schema);
        String version = starrocksQueryVisitor.getStarRocksVersion();
        this.starrocksStreamLoadVisitor = new StarRocksStreamLoadVisitor(
                sinkOptions,
                null == schema ? new String[]{} : schema.getFieldNames(),
                version.length() > 0 && !version.trim().startsWith("1.")
        );
    }

    private void validateTableStructure(TableSchema flinkSchema) {
        if (null == flinkSchema) {
            return;
        }
        Optional<UniqueConstraint> constraint = flinkSchema.getPrimaryKey();
        List<Map<String, Object>> rows = starrocksQueryVisitor.getTableColumnsMetaData();
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("Couldn't get the sink table's column info.");
        }
        // validate primary keys
        List<String> primayKeys = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            String keysType = rows.get(i).get("COLUMN_KEY").toString();
            if (!"PRI".equals(keysType)) {
                continue;
            }
            primayKeys.add(rows.get(i).get("COLUMN_NAME").toString().toLowerCase());
        }
        if (!primayKeys.isEmpty()) {
            if (!constraint.isPresent()) {
                throw new IllegalArgumentException("Primary keys not defined in the sink `TableSchema`.");
            }
            if (constraint.get().getColumns().size()!=primayKeys.size() ||
                    !constraint.get().getColumns().stream().allMatch(col -> primayKeys.contains(col.toLowerCase()))) {
                throw new IllegalArgumentException("Primary keys of the flink `TableSchema` do not match with the ones from starrocks table.");
            }
            sinkOptions.enableUpsertDelete();
        }

        if (sinkOptions.hasColumnMappingProperty()) {
            return;
        }
        if (flinkSchema.getFieldCount() != rows.size()) {
            throw new IllegalArgumentException("Fields count of " + this.sinkOptions.getTableName() + " mismatch. \nflinkSchema["
                    + flinkSchema.getFieldNames().length + "]:"
                    + Arrays.asList(flinkSchema.getFieldNames()).stream().collect(Collectors.joining(","))
                    + "\n realTab[" + rows.size() + "]:"
                    + rows.stream().map((r) -> String.valueOf(r.get("COLUMN_NAME"))).collect(Collectors.joining(",")));
        }
        List<TableColumn> flinkCols = flinkSchema.getTableColumns();
        for (int i = 0; i < rows.size(); i++) {
            String starrocksField = rows.get(i).get("COLUMN_NAME").toString().toLowerCase();
            String starrocksType = rows.get(i).get("DATA_TYPE").toString().toLowerCase();
            List<TableColumn> matchedFlinkCols = flinkCols.stream()
                    .filter(col -> col.getName().toLowerCase().equals(starrocksField) && (!typesMap.containsKey(starrocksType) || typesMap.get(starrocksType).contains(col.getType().getLogicalType().getTypeRoot())))
                    .collect(Collectors.toList());
            if (matchedFlinkCols.isEmpty()) {
                throw new IllegalArgumentException("Fields name or type mismatch for:" + starrocksField);
            }
        }
    }

    private static final Map<String, List<LogicalTypeRoot>> typesMap = new HashMap<>();

    static {
        // validate table structure
        typesMap.put("bigint", Arrays.asList(LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("largeint", Arrays.asList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("char", Arrays.asList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR));
        typesMap.put("date", Arrays.asList(LogicalTypeRoot.DATE, LogicalTypeRoot.VARCHAR));
        typesMap.put("datetime", Arrays.asList(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, LogicalTypeRoot.VARCHAR));
        typesMap.put("decimal", Arrays.asList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.DOUBLE, LogicalTypeRoot.FLOAT));
        typesMap.put("double", Arrays.asList(LogicalTypeRoot.DOUBLE, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER));
        typesMap.put("float", Arrays.asList(LogicalTypeRoot.FLOAT, LogicalTypeRoot.INTEGER));
        typesMap.put("int", Arrays.asList(LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("tinyint", Arrays.asList(LogicalTypeRoot.TINYINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY, LogicalTypeRoot.BOOLEAN));
        typesMap.put("smallint", Arrays.asList(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("varchar", Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
        typesMap.put("string", Arrays.asList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
    }

    public StarRocksStreamLoadVisitor getStarrocksStreamLoadVisitor() {
        return this.starrocksStreamLoadVisitor;
    }

}

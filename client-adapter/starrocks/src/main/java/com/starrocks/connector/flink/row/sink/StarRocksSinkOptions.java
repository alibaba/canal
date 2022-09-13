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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class StarRocksSinkOptions implements Serializable {

    private static final long serialVersionUID = 1l;
    private static final long KILO_BYTES_SCALE = 1024l;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;

    public enum StreamLoadFormat {
        CSV, JSON;
    }

    private static final String FORMAT_KEY = "format";
    
    // required sink configurations
    public static final ConfigOption<String> JDBC_URL = ConfigOptions.key("jdbc-url")
        .stringType().noDefaultValue().withDescription("Host of the stream load like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.");
    public static final ConfigOption<List<String>> LOAD_URL = ConfigOptions.key("load-url")
        .stringType().asList().noDefaultValue().withDescription("Host of the stream load like: `fe_ip1:http_port;fe_ip2:http_port;fe_ip3:http_port`.");
    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
        .stringType().noDefaultValue().withDescription("Database name of the stream load.");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
        .stringType().noDefaultValue().withDescription("Table name of the stream load.");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
        .stringType().noDefaultValue().withDescription("StarRocks user name.");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
        .stringType().noDefaultValue().withDescription("StarRocks user password.");

    // optional sink configurations
    public static final ConfigOption<String> SINK_LABEL_PREFIX = ConfigOptions.key("sink.label-prefix")
        .stringType().noDefaultValue().withDescription("The prefix of the stream load label. Available values are within [-_A-Za-z0-9]");
    public static final ConfigOption<Integer> SINK_CONNECT_TIMEOUT = ConfigOptions.key("sink.connect.timeout-ms")
        .intType().defaultValue(1000).withDescription("Timeout in millisecond for connecting to the `load-url`.");
    public static final ConfigOption<String> SINK_SEMANTIC = ConfigOptions.key("sink.semantic")
        .stringType().defaultValue(StarRocksSinkSemantic.AT_LEAST_ONCE.getName()).withDescription("Fault tolerance guarantee. `at-least-once` or `exactly-once`");
    public static final ConfigOption<Long> SINK_BATCH_MAX_SIZE = ConfigOptions.key("sink.buffer-flush.max-bytes")
        .longType().defaultValue(90L * MEGA_BYTES_SCALE).withDescription("Max data bytes of the flush.");
    public static final ConfigOption<Long> SINK_BATCH_MAX_ROWS = ConfigOptions.key("sink.buffer-flush.max-rows")
        .longType().defaultValue(500000L).withDescription("Max row count of the flush.");
    public static final ConfigOption<Long> SINK_BATCH_FLUSH_INTERVAL = ConfigOptions.key("sink.buffer-flush.interval-ms")
        .longType().defaultValue(300000L).withDescription("Flush interval of the row batch in millisecond.");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries")
        .intType().defaultValue(3).withDescription("Max flushing retry times of the row batch.");
    public static final ConfigOption<Long> SINK_BATCH_OFFER_TIMEOUT = ConfigOptions.key("sink.buffer-flush.enqueue-timeout-ms")
        .longType().defaultValue(600000L).withDescription("Offer to flushQueue timeout in millisecond.");
    public static final ConfigOption<Integer> SINK_METRIC_HISTOGRAM_WINDOW_SIZE = ConfigOptions.key("sink.metric.histogram-window-size")
        .intType().defaultValue(100).withDescription("Window size of histogram metrics.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    // Sink semantic
    private static final Set<String> SINK_SEMANTIC_ENUMS = Arrays.stream(StarRocksSinkSemantic.values()).map(s -> s.getName()).collect(Collectors.toSet());
    // wild stream load properties' prefix
    public static final String SINK_PROPERTIES_PREFIX = "sink.properties.";

    private final ReadableConfig tableOptions;
    private final Map<String, String> streamLoadProps = new HashMap<>();
    private final Map<String, String> tableOptionsMap;
    private StarRocksSinkSemantic sinkSemantic;
    private boolean supportUpsertDelete;

    public StarRocksSinkOptions(ReadableConfig options, Map<String, String> optionsMap) {
        this.tableOptions = options;
        // Can not promise the input parameter optionsMap is serializable. Use the HashMap to copy the data.
        this.tableOptionsMap = new HashMap<>(optionsMap);
        parseSinkStreamLoadProperties();
        this.validate();
    }

    private void validate() {
        validateRequired();
        validateStreamLoadUrl();
        validateSinkSemantic();
        validateParamsRange();
    }
    
    public String getJdbcUrl() {
        return tableOptions.get(JDBC_URL);
    }

    public String getDatabaseName() {
        return tableOptions.get(DATABASE_NAME);
    }

    public String getTableName() {
        return tableOptions.get(TABLE_NAME);
    }

    public String getUsername() {
        return tableOptions.get(USERNAME);
    }

    public String getPassword() {
        return tableOptions.get(PASSWORD);
    }

    public List<String> getLoadUrlList() {
        return tableOptions.getOptional(LOAD_URL).orElse(null);
    }
    
    public String getLabelPrefix() {
        return tableOptions.getOptional(SINK_LABEL_PREFIX).orElse(null);
    }

    public int getSinkMaxRetries() {
        return tableOptions.get(SINK_MAX_RETRIES).intValue();
    }

    public long getSinkMaxFlushInterval() {
        return tableOptions.get(SINK_BATCH_FLUSH_INTERVAL).longValue();
    }

    public long getSinkMaxRows() {
        return tableOptions.get(SINK_BATCH_MAX_ROWS).longValue();
    }

    public long getSinkMaxBytes() {
        return tableOptions.get(SINK_BATCH_MAX_SIZE).longValue();
    }

    public int getConnectTimeout() {
        int connectTimeout = tableOptions.get(SINK_CONNECT_TIMEOUT).intValue();
        if (connectTimeout < 100) {
            return 100;
        }
        if (connectTimeout > 60000) {
            return 60000;
        }
        return connectTimeout;
    }

    public long getSinkOfferTimeout() {
        return tableOptions.get(SINK_BATCH_OFFER_TIMEOUT).longValue();
    }

    public int getSinkHistogramWindowSize() {
        return tableOptions.get(SINK_METRIC_HISTOGRAM_WINDOW_SIZE);
    }

    public Integer getSinkParallelism() {
        return tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
    }

    public static Builder builder() {
        return new Builder();
    }
    
    public StarRocksSinkSemantic getSemantic() {
        return this.sinkSemantic;
    }

    public Map<String, String> getSinkStreamLoadProperties() {
        return streamLoadProps;
    }

    public boolean hasColumnMappingProperty() {
        return streamLoadProps.containsKey("columns");
    }

    public StreamLoadFormat getStreamLoadFormat() {
        Map<String, String> loadProsp = getSinkStreamLoadProperties();
        String format = loadProsp.get(FORMAT_KEY);
        if (null != format && StreamLoadFormat.JSON.name().equalsIgnoreCase(format)) {
            return StreamLoadFormat.JSON;
        }
        return StreamLoadFormat.CSV;
    }

    public void enableUpsertDelete() {
        supportUpsertDelete = true;
    }

    public boolean supportUpsertDelete() {
        return supportUpsertDelete;
    }

    private void validateStreamLoadUrl() {
        tableOptions.getOptional(LOAD_URL).ifPresent(urlList -> {
            for (String host : urlList) {
                if (host.split(":").length < 2) {
                    throw new ValidationException(String.format(
                        "Could not parse host '%s' in option '%s'. It should follow the format 'host_name:port'.",
                        host,
                        LOAD_URL.key()));
                }
            }
        });
    }

    private void validateSinkSemantic() {
        tableOptions.getOptional(SINK_SEMANTIC).ifPresent(semantic -> {
            if (!SINK_SEMANTIC_ENUMS.contains(semantic)){
                throw new ValidationException(
                    String.format("Unsupported value '%s' for '%s'. Supported values are ['at-least-once', 'exactly-once'].",
                        semantic, SINK_SEMANTIC.key()));
            }
        });
        this.sinkSemantic = StarRocksSinkSemantic.fromName(tableOptions.get(SINK_SEMANTIC));
    }

    private void validateParamsRange() {
        tableOptions.getOptional(SINK_MAX_RETRIES).ifPresent(val -> {
            if (val.intValue() < 0 || val.intValue() > 1000) {
                throw new ValidationException(
                    String.format("Unsupported value '%d' for '%s'. Supported value range: [0, 1000].",
                        val, SINK_MAX_RETRIES.key()));
            }
        });
        tableOptions.getOptional(SINK_BATCH_FLUSH_INTERVAL).ifPresent(val -> {
            if (val.longValue() < 1000l || val.longValue() > 3600000l) {
                throw new ValidationException(
                    String.format("Unsupported value '%d' for '%s'. Supported value range: [1000, 3600000].",
                        val, SINK_BATCH_FLUSH_INTERVAL.key()));
            }
        });
        tableOptions.getOptional(SINK_BATCH_MAX_ROWS).ifPresent(val -> {
            if (val.longValue() < 64000 || val.longValue() > 5000000) {
                throw new ValidationException(
                    String.format("Unsupported value '%d' for '%s'. Supported value range: [64000, 5000000].",
                        val, SINK_BATCH_MAX_ROWS.key()));
            }
        });
        tableOptions.getOptional(SINK_BATCH_MAX_SIZE).ifPresent(val -> {
            if (val.longValue() < 64 * MEGA_BYTES_SCALE || val.longValue() > 10 * GIGA_BYTES_SCALE) {
                throw new ValidationException(
                    String.format("Unsupported value '%d' for '%s'. Supported value range: [%d, %d].",
                        val, SINK_BATCH_MAX_SIZE.key(), 64 * MEGA_BYTES_SCALE, 10 * GIGA_BYTES_SCALE));
            }
        });
        tableOptions.getOptional(SINK_BATCH_OFFER_TIMEOUT).ifPresent(val -> {
            if (val.longValue() < 300000 || val.longValue() > Long.MAX_VALUE) {
                throw new ValidationException(
                    String.format("Unsupported value '%d' for '%s'. Supported value range: [300000, Long.MAX_VALUE].",
                        val, SINK_BATCH_OFFER_TIMEOUT.key()));
            }
        });
    }

    private void validateRequired() {
        ConfigOption<?>[] configOptions = new ConfigOption[]{
            USERNAME,
            PASSWORD,
            TABLE_NAME,
            DATABASE_NAME,
            JDBC_URL,
            LOAD_URL
        };
        int presentCount = 0;
        for (ConfigOption<?> configOption : configOptions) {
            if (tableOptions.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
            "Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
    }

    private void parseSinkStreamLoadProperties() {
        tableOptionsMap.keySet().stream()
            .filter(key -> key.startsWith(SINK_PROPERTIES_PREFIX))
            .forEach(key -> {
                final String value = tableOptionsMap.get(key);
                final String subKey = key.substring((SINK_PROPERTIES_PREFIX).length()).toLowerCase();
                streamLoadProps.put(subKey, value);
            });
    }

    /**
    * Builder for {@link StarRocksSinkOptions}.
    */
    public static final class Builder {
        private final Configuration conf;
        public Builder() {
            conf = new Configuration();
        }

        public Builder withProperty(String key, String value) {
            conf.setString(key, value);
            return this;
        }

        public StarRocksSinkOptions build() {
            return new StarRocksSinkOptions(conf, conf.toMap());
        }
    }

}

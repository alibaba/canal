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

import com.alibaba.fastjson2.JSON;
import com.starrocks.connector.flink.row.sink.StarRocksDelimiterParser;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOptions;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class StarRocksStreamLoadVisitor implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadVisitor.class);

    private static final int ERROR_LOG_MAX_LENGTH = 3000;

    private StarRocksSinkOptions sinkOptions;
    private final String[] fieldNames;
    private long pos;
    private boolean __opAutoProjectionInJson;
    private static final String RESULT_FAILED = "Fail";
    private static final String RESULT_LABEL_EXISTED = "Label Already Exists";
    private static final String LAEBL_STATE_VISIBLE = "VISIBLE";
    private static final String LAEBL_STATE_COMMITTED = "COMMITTED";
    private static final String RESULT_LABEL_PREPARE = "PREPARE";
    private static final String RESULT_LABEL_ABORTED = "ABORTED";
    private static final String RESULT_LABEL_UNKNOWN = "UNKNOWN";

    public StarRocksStreamLoadVisitor(StarRocksSinkOptions sinkOptions, String[] fieldNames, boolean __opAutoProjectionInJson) {
        this.fieldNames = fieldNames;
        this.sinkOptions = sinkOptions;
        this.__opAutoProjectionInJson = __opAutoProjectionInJson;
    }

    public void setSinkOptions(StarRocksSinkOptions sinkOptions) {
        this.sinkOptions =  sinkOptions;
    }

    public Map<String, Object> doStreamLoad(StarRocksSinkBufferEntity bufferEntity) throws IOException {
        String host = getAvailableHost();
        if (null == host) {
            throw new IOException("None of the hosts in `load_url` could be connected.");
        }
        String loadUrl = new StringBuilder(host)
            .append("/api/")
            .append(bufferEntity.getDatabase())
            .append("/")
            .append(bufferEntity.getTable())
            .append("/_stream_load")
            .toString();
        LOG.info(String.format("Start to join batch data: label[%s].", bufferEntity.getLabel()));
        Map<String, Object> loadResult = doHttpPut(loadUrl, bufferEntity.getLabel(), joinRows(bufferEntity.getBuffer(),  (int) bufferEntity.getBatchSize()));

        final String keyStatus = "Status";
        if (null == loadResult || !loadResult.containsKey(keyStatus)) {
            throw new IOException("Unable to flush data to StarRocks: unknown result status, usually caused by: 1.authorization or permission related problems. 2.Wrong column_separator or row_delimiter. 3.Column count exceeded the limitation.");
        }
        Integer numberFilteredRows = (Integer) loadResult.get("NumberFilteredRows");
        if (numberFilteredRows != 0) {
            LOG.warn("Filter table {} data rows: {}", bufferEntity.getDatabase() + "." + bufferEntity.getTable(), numberFilteredRows);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
        }
        if (RESULT_FAILED.equals(loadResult.get(keyStatus))) {
            Map<String, String> logMap = new HashMap<>();
            if (loadResult.containsKey("ErrorURL")) {
                logMap.put("streamLoadErrorLog", getErrorLog((String) loadResult.get("ErrorURL")));
            }
            throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                "response: \n%s\n%s\n", JSON.toJSONString(loadResult), JSON.toJSONString(logMap)), loadResult);
        } else if (RESULT_LABEL_EXISTED.equals(loadResult.get(keyStatus))) {
            LOG.error(String.format("Stream Load response: \n%s\n", JSON.toJSONString(loadResult)));
            // has to block-checking the state to get the final result
            checkLabelState(host, bufferEntity.getLabel());
        }
        return loadResult;
    }

    @SuppressWarnings("unchecked")
    private void checkLabelState(String host, String label) throws IOException {
        int idx = 0;
        while(true) {
            try {
                TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            } catch (InterruptedException ex) {
                break;
            }
            try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
                HttpGet httpGet = new HttpGet(new StringBuilder(host).append("/api/").append(sinkOptions.getDatabaseName()).append("/get_load_state?label=").append(label).toString());
                httpGet.setHeader("Authorization", getBasicAuthHeader(sinkOptions.getUsername(), sinkOptions.getPassword()));
                httpGet.setHeader("Connection", "close");

                try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                    HttpEntity respEntity = getHttpEntity(resp);
                    if (respEntity == null) {
                        throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label[%s].\n", label), null);
                    }
                    Map<String, Object> result = (Map<String, Object>)JSON.parse(EntityUtils.toString(respEntity));
                    String labelState = (String)result.get("state");
                    if (null == labelState) {
                        throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                "could not get the final state of label[%s]. response[%s]\n", label, EntityUtils.toString(respEntity)), null);
                    }
                    LOG.info(String.format("Checking label[%s] state[%s]\n", label, labelState));
                    switch(labelState) {
                        case LAEBL_STATE_VISIBLE:
                        case LAEBL_STATE_COMMITTED:
                            return;
                        case RESULT_LABEL_PREPARE:
                            continue;
                        case RESULT_LABEL_ABORTED:
                            throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                    "label[%s] state[%s]\n", label, labelState), null, true);
                        case RESULT_LABEL_UNKNOWN:
                        default:
                            throw new StarRocksStreamLoadFailedException(String.format("Failed to flush data to StarRocks, Error " +
                                "label[%s] state[%s]\n", label, labelState), null);
                    }
                }
            }
        }
    }

    private String getErrorLog(String errorUrl) {
        if (errorUrl == null || errorUrl.isEmpty() || !errorUrl.startsWith("http")) {
            return null;
        }
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(errorUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null) {
                    return null;
                }
                String errorLog = EntityUtils.toString(respEntity);
                if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                    errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
                }
                return errorLog;
            }
        } catch (Exception e) {
            LOG.warn("Failed to get error log.", e);
            return "Failed to get error log: " + e.getMessage();
        }
    }

    private String getAvailableHost() {
        List<String> hostList = sinkOptions.getLoadUrlList();
        long tmp = pos + hostList.size();
        while (pos < tmp) {
            String host = "http://" + hostList.get((int) (pos % hostList.size()));
            if (tryHttpConnection(host)) {
                pos++;
                return host;
            }
        }

        return null;
    }

    private boolean tryHttpConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co =  (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(sinkOptions.getConnectTimeout());
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            LOG.warn("Failed to connect to address:{}", host, e1);
            return false;
        }
    }

    private byte[] joinRows(List<byte[]> rows, int totalBytes) throws IOException {
        if (StarRocksSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            byte[] lineDelimiter = StarRocksDelimiterParser.parse(sinkOptions.getSinkStreamLoadProperties().get("row_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (StarRocksSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new RuntimeException("Failed to join rows data, unsupported `format` from stream load properties:");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String loadUrl, String label, byte[] data) throws IOException {
        LOG.info(String.format("Executing stream load to: '%s', size: '%s', thread: %d", loadUrl, data.length, Thread.currentThread().getId()));
        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });
        try (CloseableHttpClient httpclient = httpClientBuilder.build()) {
            HttpPut httpPut = new HttpPut(loadUrl);
            Map<String, String> props = sinkOptions.getSinkStreamLoadProperties();
            for (Map.Entry<String,String> entry : props.entrySet()) {
                httpPut.setHeader(entry.getKey(), entry.getValue());
            }
            if (!props.containsKey("columns") && ((sinkOptions.supportUpsertDelete() && !__opAutoProjectionInJson) || StarRocksSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat()))) {
                String cols = String.join(",", Arrays.asList(fieldNames).stream().map(f -> String.format("`%s`", f.trim().replace("`", ""))).collect(Collectors.toList()));
                if (cols.length() > 0 && sinkOptions.supportUpsertDelete()) {
                    cols += String.format(",%s", StarRocksSinkOP.COLUMN_KEY);
                }
                httpPut.setHeader("columns", cols);
            }
            if (!httpPut.containsHeader("timeout")) {
                httpPut.setHeader("timeout", "60");
            }
            httpPut.setHeader("Expect", "100-continue");
            httpPut.setHeader("label", label);
            httpPut.setHeader("Authorization", getBasicAuthHeader(sinkOptions.getUsername(), sinkOptions.getPassword()));
            httpPut.setEntity(new ByteArrayEntity(data));
            httpPut.setConfig(RequestConfig.custom().setRedirectsEnabled(true).build());
            try (CloseableHttpResponse resp = httpclient.execute(httpPut)) {
                HttpEntity respEntity = getHttpEntity(resp);
                if (respEntity == null)
                    return null;
                return (Map<String, Object>)JSON.parse(EntityUtils.toString(respEntity));
            }
        }
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

    private HttpEntity getHttpEntity(CloseableHttpResponse resp) {
        int code = resp.getStatusLine().getStatusCode();
        if (200 != code) {
            LOG.warn("Request failed with code:{}", code);
            return null;
        }
        HttpEntity respEntity = resp.getEntity();
        if (null == respEntity) {
            LOG.warn("Request failed with empty response.");
            return null;
        }
        return respEntity;
    }
}

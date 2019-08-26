package com.alibaba.otter.canal.deployer.monitor.remote.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.otter.canal.deployer.monitor.remote.ConfigItem;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.apache.http.client.config.RequestConfig.custom;

public class HttpHelper {

    private final static Logger logger = LoggerFactory.getLogger(HttpRemoteConfigLoader.class);

    private CloseableHttpClient httpclient;

    public HttpHelper() {
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setMaxConnPerRoute(50);
        builder.setMaxConnTotal(100);
        httpclient = builder.build();
    }

    public String get(String url, Map<String, String> heads, int timeout) {
        // 支持采用https协议，忽略证书
        url = url.trim();
        if (url.startsWith("https")) {
            // FIXME
            return "";
        }
        CloseableHttpResponse response = null;
        HttpGet httpGet = null;
        try {
            URI uri = new URIBuilder(url).build();
            RequestConfig config = custom().setConnectTimeout(timeout)
                    .setConnectionRequestTimeout(timeout)
                    .setSocketTimeout(timeout)
                    .build();
            httpGet = new HttpGet(uri);
            if (heads != null) {
                for (Map.Entry<String, String> entry : heads.entrySet()) {
                    httpGet.setHeader(entry.getKey(), entry.getValue());
                }
            }

            HttpClientContext context = HttpClientContext.create();
            context.setRequestConfig(config);
            response = httpclient.execute(httpGet, context);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                return EntityUtils.toString(response.getEntity());
            } else {
                String errorMsg = EntityUtils.toString(response.getEntity());
                throw new RuntimeException("requestGet remote error, url=" + uri.toString() + ", code=" + statusCode
                        + ", error msg=" + errorMsg);
            }
        } catch (Throwable t) {
            throw new RuntimeException("requestGet remote error, request : " + url, t);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
    }

    public String post(String url, Map<String, String> heads, Object requestBody, int timeout) {
        String json = JSON.toJSONString(requestBody);
        return post0(url, heads, json, timeout);
    }

    public String post0(String url, Map<String, String> heads, String requestBody, int timeout) {
        url = url.trim();
        // 支持采用https协议，忽略证书
        if (url.startsWith("https")) {
            // FIXME
            return "";
        }
        HttpPost httpPost = null;
        CloseableHttpResponse response = null;
        try {
            URI uri = new URIBuilder(url).build();
            RequestConfig config = custom().setConnectTimeout(timeout)
                    .setConnectionRequestTimeout(timeout)
                    .setSocketTimeout(timeout)
                    .build();
            httpPost = new HttpPost(uri);
            StringEntity entity = new StringEntity(requestBody, "UTF-8");
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json;charset=utf8");
            if (heads != null) {
                for (Map.Entry<String, String> entry : heads.entrySet()) {
                    httpPost.setHeader(entry.getKey(), entry.getValue());
                }
            }

            HttpClientContext context = HttpClientContext.create();
            context.setRequestConfig(config);

            response = httpclient.execute(httpPost, context);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                return EntityUtils.toString(response.getEntity());
            } else {
                throw new RuntimeException("requestPost remote error, request : " + url + ", statusCode=" + statusCode
                        + ";" + EntityUtils.toString(response.getEntity()));
            }
        } catch (Throwable t) {
            throw new RuntimeException("requestPost remote error, request : " + url, t);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
    }

    public void close() {
        if (httpclient != null) {
            try {
                httpclient.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}

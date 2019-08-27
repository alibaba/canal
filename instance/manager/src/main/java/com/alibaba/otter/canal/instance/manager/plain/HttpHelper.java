package com.alibaba.otter.canal.instance.manager.plain;

import static org.apache.http.client.config.RequestConfig.custom;

import java.io.IOException;
import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * http client 工具类
 *
 * @author rewerma 2019-08-26 上午09:40:36
 * @version 1.0.0
 */
public class HttpHelper {

    private final static Logger logger                   = LoggerFactory.getLogger(HttpHelper.class);

    public static final Integer REST_STATE_OK            = 20000;
    public static final Integer REST_STATE_TOKEN_INVALID = 50014;
    public static final Integer REST_STATE_ERROR         = 50000;

    private CloseableHttpClient httpclient;

    public HttpHelper(){
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setMaxConnPerRoute(50);
        builder.setMaxConnTotal(100);

        // 创建支持忽略证书的https
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {

                @Override
                public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                    return true;
                }
            }).build();

            httpclient = HttpClientBuilder.create()
                .setSSLContext(sslContext)
                .setConnectionManager(new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory> create()
                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                    .register("https", new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE))
                    .build()))
                .build();
        } catch (Throwable e) {
            // ignore
        }
    }

    public String get(String url, Map<String, String> heads, int timeout) {
        url = url.trim();
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
        return post0(url, heads, JSON.toJSONString(requestBody), timeout);
    }

    public String post0(String url, Map<String, String> heads, String requestBody, int timeout) {
        url = url.trim();
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

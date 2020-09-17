package com.alibaba.otter.canal.parse.inbound.mysql.rds.request;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

/**
 * @author chengjin.lyf on 2018/8/7 下午2:26
 * @since 1.0.25
 */
public abstract class AbstractRequest<T> {

    /**
     * 要求的编码格式
     */
    private static final String ENCODING = "UTF-8";
    /**
     * 要求的sign签名算法
     */
    private static final String MAC_NAME = "HmacSHA1";

    private String              accessKeyId;

    private String              accessKeySecret;

    /**
     * api 版本
     */
    private String              version;

    private String              endPoint = "rds.aliyuncs.com";

    private String              protocol = "http";

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    private int                 timeout = (int) TimeUnit.MINUTES.toMillis(1);

    private Map<String, String> treeMap = new TreeMap();

    public void putQueryString(String name, String value) {
        if (StringUtils.isBlank(name) || StringUtils.isBlank(value)) {
            return;
        }
        treeMap.put(name, value);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    /**
     * 使用 HMAC-SHA1 签名方法对对encryptText进行签名
     *
     * @param encryptText 被签名的字符串
     * @param encryptKey 密钥
     * @return
     * @throws Exception
     */
    private byte[] HmacSHA1Encrypt(String encryptText, String encryptKey) throws Exception {
        byte[] data = encryptKey.getBytes(ENCODING);
        // 根据给定的字节数组构造一个密钥,第二参数指定一个密钥算法的名称
        SecretKey secretKey = new SecretKeySpec(data, MAC_NAME);
        // 生成一个指定 Mac 算法 的 Mac 对象
        Mac mac = Mac.getInstance(MAC_NAME);
        // 用给定密钥初始化 Mac 对象
        mac.init(secretKey);

        byte[] text = encryptText.getBytes(ENCODING);
        // 完成 Mac 操作
        return mac.doFinal(text);
    }

    private String base64(byte input[]) throws UnsupportedEncodingException {
        return new String(Base64.encodeBase64(input), ENCODING);
    }

    private String concatQueryString(Map<String, String> parameters) throws UnsupportedEncodingException {
        if (null == parameters) {
            return null;
        }
        StringBuilder urlBuilder = new StringBuilder("");
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            urlBuilder.append(encode(key));
            if (val != null) {
                urlBuilder.append("=").append(encode(val));
            }
            urlBuilder.append("&");
        }
        int strIndex = urlBuilder.length();
        if (parameters.size() > 0) {
            urlBuilder.deleteCharAt(strIndex - 1);
        }
        return urlBuilder.toString();
    }

    private String encode(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }

    private String makeSignature(TreeMap<String, String> paramMap) throws Exception {
        String cqs = concatQueryString(paramMap);
        cqs = encode(cqs);
        cqs = cqs.replaceAll("\\+", "%20");
        cqs = cqs.replaceAll("\\*", "%2A");
        cqs = cqs.replaceAll("%7E", "~");
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("GET").append("&").append(encode("/")).append("&").append(cqs);
        return base64(HmacSHA1Encrypt(stringBuilder.toString(), accessKeySecret + "&"));
    }

    public final String formatUTCTZ(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

    private void fillCommonParam(Map<String, String> p) {
        p.put("Format", "JSON");
        p.put("Version", version);
        p.put("AccessKeyId", accessKeyId);
        p.put("SignatureMethod", "HMAC-SHA1"); // 此处不能用变量 MAC_NAME
        p.put("Timestamp", formatUTCTZ(new Date()));
        p.put("SignatureVersion", "1.0");
        p.put("SignatureNonce", UUID.randomUUID().toString());
    }

    private String makeRequestString(Map<String, String> param) throws Exception {
        fillCommonParam(param);
        String sign = makeSignature(new TreeMap<>(param));
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : param.entrySet()) {
            builder.append(encode(entry.getKey())).append("=").append(encode(entry.getValue())).append("&");
        }
        builder.append("Signature").append("=").append(sign);
        return builder.toString();
    }

    /**
     * 执行http请求
     *
     * @param getMethod
     * @return
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    private final HttpResponse executeHttpRequest(HttpGet getMethod, String host) throws Exception {
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, (TrustStrategy) (arg0, arg1) -> true).build();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
            new String[] { "TLSv1" },
            null,
            SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        Registry registry = RegistryBuilder.create()
            .register("http", PlainConnectionSocketFactory.INSTANCE)
            .register("https", sslsf)
            .build();
        HttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager(registry);
        CloseableHttpClient httpClient = HttpClientBuilder.create()
            .setMaxConnPerRoute(50)
            .setMaxConnTotal(100)
            .setConnectionManager(httpClientConnectionManager)
            .build();
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(timeout)
            .setConnectionRequestTimeout(timeout)
            .setSocketTimeout(timeout)
            .build();
        getMethod.setConfig(requestConfig);
        HttpResponse response = httpClient.execute(getMethod);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpResponseStatus.OK.code() && statusCode != HttpResponseStatus.PARTIAL_CONTENT.code()) {
            String result = EntityUtils.toString(response.getEntity());
            throw new RuntimeException("return error !" + response.getStatusLine().getReasonPhrase() + ", " + result);
        }
        return response;
    }

    protected abstract T processResult(HttpResponse response) throws Exception;

    protected void processBefore() {

    }

    public final T doAction() throws Exception {
        processBefore();
        String requestStr = makeRequestString(treeMap);
        HttpGet httpGet = new HttpGet(protocol + "://" + endPoint + "?" + requestStr);
        HttpResponse response = executeHttpRequest(httpGet, endPoint);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            String result = EntityUtils.toString(response.getEntity());
            throw new RuntimeException("http request failed! " + result);
        }
        return processResult(response);
    }
}

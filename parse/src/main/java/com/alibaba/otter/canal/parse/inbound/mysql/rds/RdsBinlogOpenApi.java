package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @author agapple 2017年10月14日 下午1:53:52
 * @since 1.0.25
 */
public class RdsBinlogOpenApi {

    protected static final Logger logger              = LoggerFactory.getLogger(RdsBinlogOpenApi.class);
    private static final String   ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final int      TIMEOUT             = 10000;
    private static final String   ENCODING            = "UTF-8";
    private static final String   MAC_NAME            = "HmacSHA1";
    private static final String   API_VERSION         = "2014-08-15";
    private static final String   SIGNATURE_VERSION   = "1.0";

    public static void downloadBinlogFiles(String url, String ak, String sk, String dbInstanceId, Date startTime,
                                           Date endTime, File destDir) throws Throwable {
        int pageSize = 100;
        int pageNumber = 0;
        int pageRecordCount = 1;
        String hostInstanceID = null;
        while (pageRecordCount > 0 && pageRecordCount <= pageSize) {
            pageNumber += 1;
            String result = describeBinlogFiles(url, ak, sk, dbInstanceId, startTime, endTime, pageSize, pageNumber);
            JSONObject jsobObj = JSON.parseObject(result);
            pageRecordCount = jsobObj.getInteger("PageRecordCount");

            if (pageRecordCount > 0) {
                FileUtils.forceMkdir(destDir);
                File hostIdFile = new File(destDir, "hostId");
                if (hostIdFile.exists()) {
                    List<String> lines = IOUtils.readLines(new FileInputStream(hostIdFile));
                    hostInstanceID = StringUtils.join(lines, "\n");
                }

                String itemStr = jsobObj.getString("Items");
                JSONObject binLogFileObj = JSONObject.parseObject(itemStr);
                JSONArray items = binLogFileObj.getJSONArray("BinLogFile");
                if (items == null || items.isEmpty()) {
                    continue;
                }
                for (int i = 0; i < items.size(); i++) {
                    JSONObject item = (JSONObject) items.get(i);
                    String oneHostInstanceID = item.getString("HostInstanceID");
                    if (hostInstanceID == null) {
                        hostInstanceID = oneHostInstanceID;
                        FileOutputStream hostIdFileOut = null;
                        try {
                            hostIdFileOut = new FileOutputStream(hostIdFile);
                            hostIdFileOut.write(oneHostInstanceID.getBytes());
                            hostIdFileOut.flush();
                        } finally {
                            IOUtils.closeQuietly(hostIdFileOut);
                        }
                    }

                    if (hostInstanceID.equals(oneHostInstanceID)) { // 只选择一个host下载
                        String downloadLink = item.getString("DownloadLink");
                        String fileName = StringUtils.substringBetween(downloadLink, "mysql-bin.", ".tar");
                        if (StringUtils.isNotEmpty(fileName)) {
                            File currentFile = new File(destDir, "mysql-bin." + fileName);
                            if (currentFile.isFile() && currentFile.exists()) {
                                // 检查一下文件是否存在，存在就就没必要下载了
                                continue;
                            }
                        }

                        HttpGet httpGet = new HttpGet(downloadLink);
                        CloseableHttpClient httpClient = HttpClientBuilder.create()
                            .setMaxConnPerRoute(50)
                            .setMaxConnTotal(100)
                            .build();
                        RequestConfig requestConfig = RequestConfig.custom()
                            .setConnectTimeout(TIMEOUT)
                            .setConnectionRequestTimeout(TIMEOUT)
                            .setSocketTimeout(TIMEOUT)
                            .build();
                        httpGet.setConfig(requestConfig);
                        HttpResponse response = httpClient.execute(httpGet);
                        int statusCode = response.getStatusLine().getStatusCode();
                        if (statusCode != HttpResponseStatus.OK.code()) {
                            throw new RuntimeException("download failed , url:" + downloadLink + " , statusCode:"
                                                       + statusCode);
                        }
                        saveFile(destDir, response);
                    }
                }
            }
        }
    }

    private static void saveFile(File parentFile, HttpResponse response) throws IOException {
        InputStream is = response.getEntity().getContent();
        long totalSize = Long.parseLong(response.getFirstHeader("Content-Length").getValue());
        String fileName = response.getFirstHeader("Content-Disposition").getValue();
        fileName = StringUtils.substringAfter(fileName, "filename=");
        boolean isTar = StringUtils.endsWith(fileName, ".tar");
        FileUtils.forceMkdir(parentFile);
        FileOutputStream fos = null;
        try {
            if (isTar) {
                TarArchiveInputStream tais = new TarArchiveInputStream(is);
                TarArchiveEntry tarArchiveEntry = null;
                while ((tarArchiveEntry = tais.getNextTarEntry()) != null) {
                    String name = tarArchiveEntry.getName();
                    File tarFile = new File(parentFile, name);
                    logger.info("start to download file " + tarFile.getName());
                    BufferedOutputStream bos = null;
                    try {
                        bos = new BufferedOutputStream(new FileOutputStream(tarFile));
                        int read = -1;
                        byte[] buffer = new byte[1024];
                        while ((read = tais.read(buffer)) != -1) {
                            bos.write(buffer, 0, read);
                        }
                        logger.info("download file " + tarFile.getName() + " end!");
                    } finally {
                        IOUtils.closeQuietly(bos);
                    }
                }
                tais.close();
            } else {
                File file = new File(parentFile, fileName);
                if (!file.isFile()) {
                    file.createNewFile();
                }
                try {
                    fos = new FileOutputStream(file);
                    byte[] buffer = new byte[1024];
                    int len;
                    long copySize = 0;
                    long nextPrintProgress = 0;
                    logger.info("start to download file " + file.getName());
                    while ((len = is.read(buffer)) != -1) {
                        fos.write(buffer, 0, len);
                        copySize += len;
                        long progress = copySize * 100 / totalSize;
                        if (progress >= nextPrintProgress) {
                            logger.info("download " + file.getName() + " progress : " + progress
                                        + "% , download size : " + copySize + ", total size : " + totalSize);
                            nextPrintProgress += 10;
                        }
                    }
                    logger.info("download file " + file.getName() + " end!");
                    fos.flush();
                } finally {
                    IOUtils.closeQuietly(fos);
                }
            }
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    public static String describeBinlogFiles(String url, String ak, String sk, String dbInstanceId, Date startTime,
                                             Date endTime, int pageSize, int pageNumber) throws Exception {
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("Action", "DescribeBinlogFiles");
        paramMap.put("DBInstanceId", dbInstanceId); // rds实例id
        paramMap.put("StartTime", formatIso8601Date(startTime));
        paramMap.put("EndTime", formatIso8601Date(endTime));
        paramMap.put("PageSize", String.valueOf(pageSize));
        paramMap.put("PageNumber", String.valueOf(pageNumber));
        return doRequest(url, paramMap, ak, sk);
    }

    private static String doRequest(String domin, Map<String, String> param, String ak, String sk) throws Exception {
        param.put("AccessKeyId", ak);
        param.put("SignatureMethod", "HMAC-SHA1");
        param.put("SignatureVersion", SIGNATURE_VERSION);
        param.put("Version", API_VERSION);
        param.put("SignatureNonce", UUID.randomUUID().toString());
        param.put("Format", "JSON");
        param.put("Timestamp", formatIso8601Date(new Date()));
        String signStr = generate("POST", param, sk);
        param.put("Signature", signStr);
        String request = concatQueryString(param);
        String url = domin + "?" + request;
        String result = HttpHelper.post(url, null, Collections.EMPTY_MAP, TIMEOUT);
        return result;
    }

    public static String concatQueryString(Map<String, String> parameters) throws UnsupportedEncodingException {
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

    public static String encode(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }

    private static String formatIso8601Date(Date date) {
        SimpleDateFormat df = new SimpleDateFormat(ISO8601_DATE_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        return df.format(date);
    }

    /**
     * 使用 HMAC-SHA1 签名方法对对encryptText进行签名
     *
     * @param encryptText 被签名的字符串
     * @param encryptKey 密钥
     * @return
     * @throws Exception
     */
    public static byte[] HmacSHA1Encrypt(String encryptText, String encryptKey) throws Exception {
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

    private static String base64(byte input[]) throws UnsupportedEncodingException {
        return new String(Base64.encodeBase64(input), ENCODING);
    }

    /** 对参数名称和参数值进行URL编码 **/
    public static String generate(String method, Map<String, String> parameter, String accessKeySecret)
                                                                                                       throws Exception {
        String signString = generateSignString(method, parameter);
        byte[] signBytes = HmacSHA1Encrypt(signString, accessKeySecret + "&");
        String signature = base64(signBytes);
        if ("POST".equals(method)) {
            return signature;
        }
        return URLEncoder.encode(signature, "UTF-8");
    }

    private static String generateQueryString(TreeMap<String, String> treeMap) {
        StringBuilder canonicalizedQueryString = new StringBuilder();
        boolean first = true;
        for (String key : treeMap.navigableKeySet()) {
            String value = treeMap.get(key);
            if (!first) {
                canonicalizedQueryString.append("&");
            }
            first = false;
            canonicalizedQueryString.append(percentEncode(key)).append("=").append(percentEncode(value));
        }
        return canonicalizedQueryString.toString();
    }

    public static String generateSignString(String httpMethod, Map<String, String> parameter) throws IOException {
        TreeMap<String, String> sortParameter = new TreeMap<String, String>();
        sortParameter.putAll(parameter);
        String canonicalizedQueryString = generateQueryString(sortParameter);
        if (null == httpMethod) {
            throw new RuntimeException("httpMethod can not be empty");
        }
        /** 构造待签名的字符串* */
        StringBuilder stringToSign = new StringBuilder();
        stringToSign.append(httpMethod).append("&");
        stringToSign.append(percentEncode("/")).append("&");
        stringToSign.append(percentEncode(canonicalizedQueryString));
        return stringToSign.toString();
    }

    public static String percentEncode(String value) {
        try {
            return value == null ? null : URLEncoder.encode(value, ENCODING)
                .replaceAll("\\+", "%20")
                .replaceAll("\\*", "%2A")
                .replaceAll("%7E", "~");
        } catch (Exception e) {
        }
        return "";
    }
}

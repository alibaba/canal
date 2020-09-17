package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.net.ssl.SSLContext;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;

/**
 * @author chengjin.lyf on 2018/8/7 下午3:10
 * @since 1.0.25
 */
public class BinlogDownloadQueue {

    private static final Logger             logger        = LoggerFactory.getLogger(BinlogDownloadQueue.class);
    private static final int                TIMEOUT       = 10000;

    private LinkedBlockingQueue<BinlogFile> downloadQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Runnable>   taskQueue     = new LinkedBlockingQueue<>();
    private LinkedList<BinlogFile>          binlogList;
    private final int                       batchFileSize;
    private Thread                          downloadThread;
    public boolean                          running       = true;
    private final String                    destDir;
    private String                          hostId;
    private int                             currentSize;
    private String                          lastDownload;

    public BinlogDownloadQueue(List<BinlogFile> downloadQueue, int batchFileSize, String destDir) throws IOException{
        this.binlogList = new LinkedList(downloadQueue);
        this.batchFileSize = batchFileSize;
        this.destDir = destDir;
        this.currentSize = 0;
        prepareBinlogList();
        cleanDir();
    }

    private void prepareBinlogList() {
        for (BinlogFile binlog : this.binlogList) {
            String fileName = StringUtils.substringBetween(binlog.getDownloadLink(), "mysql-bin.", "?");
            binlog.setFileName(fileName);
        }
        this.binlogList.sort(Comparator.comparing(BinlogFile::getFileName));
    }

    public void cleanDir() throws IOException {
        File destDirFile = new File(destDir);
        FileUtils.forceMkdir(destDirFile);
        FileUtils.cleanDirectory(destDirFile);
    }

    public void silenceDownload() {
        if (downloadThread != null) {
            return;
        }
        downloadThread = new Thread(new DownloadThread(), "download-" + destDir);
        downloadThread.setDaemon(true);
        downloadThread.start();
    }

    public BinlogFile tryOne() throws Throwable {
        BinlogFile binlogFile = binlogList.poll();
        if (binlogFile == null) {
            throw new CanalParseException("download binlog is null");
        }
        download(binlogFile);
        hostId = binlogFile.getHostInstanceID();
        this.currentSize++;
        return binlogFile;
    }

    public void notifyNotMatch() {
        this.currentSize--;
        filter(hostId);
    }

    private void filter(String hostInstanceId) {
        Iterator<BinlogFile> it = binlogList.iterator();
        while (it.hasNext()) {
            BinlogFile bf = it.next();
            if (bf.getHostInstanceID().equalsIgnoreCase(hostInstanceId)) {
                it.remove();
            } else {
                hostId = bf.getHostInstanceID();
            }
        }
    }

    public boolean isLastFile(String fileName) {
        String needCompareName = lastDownload;
        if (StringUtils.isNotEmpty(needCompareName) && StringUtils.endsWith(needCompareName, "tar")) {
            needCompareName = needCompareName.substring(0, needCompareName.indexOf("."));
        }
        return (needCompareName == null || fileName.equalsIgnoreCase(needCompareName)) && binlogList.isEmpty();
    }

    public void prepare() throws InterruptedException {
        for (int i = this.currentSize; i < batchFileSize && !binlogList.isEmpty(); i++) {
            BinlogFile binlogFile = null;
            while (!binlogList.isEmpty()) {
                binlogFile = binlogList.poll();
                if (!binlogFile.getHostInstanceID().equalsIgnoreCase(hostId)) {
                    continue;
                }
                break;
            }
            if (binlogFile == null) {
                break;
            }
            this.downloadQueue.put(binlogFile);
            this.lastDownload = "mysql-bin." + binlogFile.getFileName();
            this.currentSize++;
        }
    }

    public void downOne() {
        this.currentSize--;
    }

    public void release() {
        running = false;
        this.currentSize = 0;
        binlogList.clear();
        downloadQueue.clear();
        try {
            downloadThread.interrupt();
            downloadThread.join();// 等待其结束
        } catch (InterruptedException e) {
            // ignore
        } finally {
            downloadThread = null;
        }
    }

    private void download(BinlogFile binlogFile) throws Throwable {
        String downloadLink = binlogFile.getDownloadLink();
        String fileName = binlogFile.getFileName();

        downloadLink = downloadLink.trim();
        CloseableHttpClient httpClient = null;
        if (downloadLink.startsWith("https")) {
            HttpClientBuilder builder = HttpClientBuilder.create();
            builder.setMaxConnPerRoute(50);
            builder.setMaxConnTotal(100);
            // 创建支持忽略证书的https
            final SSLContext sslContext = new SSLContextBuilder()
                    .loadTrustMaterial(null, (x509Certificates, s) -> true)
                    .build();

            httpClient = HttpClientBuilder.create()
                .setSSLContext(sslContext)
                .setConnectionManager(new PoolingHttpClientConnectionManager(RegistryBuilder.<ConnectionSocketFactory> create()
                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                    .register("https", new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE))
                    .build()))
                .build();
        } else {
            httpClient = HttpClientBuilder.create().setMaxConnPerRoute(50).setMaxConnTotal(100).build();
        }

        HttpGet httpGet = new HttpGet(downloadLink);
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(TIMEOUT)
            .setConnectionRequestTimeout(TIMEOUT)
            .setSocketTimeout(TIMEOUT)
            .build();
        httpGet.setConfig(requestConfig);
        HttpResponse response = httpClient.execute(httpGet);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpResponseStatus.OK.code()) {
            throw new RuntimeException("download failed , url:" + downloadLink + " , statusCode:" + statusCode);
        }
        saveFile(new File(destDir), "mysql-bin." + fileName, response);
    }

    private static void saveFile(File parentFile, String fileName, HttpResponse response) throws IOException {
        InputStream is = response.getEntity().getContent();
        long totalSize = Long.parseLong(response.getFirstHeader("Content-Length").getValue());
        if (response.getFirstHeader("Content-Disposition") != null) {
            fileName = response.getFirstHeader("Content-Disposition").getValue();
            fileName = StringUtils.substringAfter(fileName, "filename=");
        }
        boolean isTar = StringUtils.endsWith(fileName, ".tar");
        FileUtils.forceMkdir(parentFile);
        FileOutputStream fos = null;
        try {
            if (isTar) {
                TarArchiveInputStream tais = new TarArchiveInputStream(is);
                TarArchiveEntry tarArchiveEntry = null;
                while ((tarArchiveEntry = tais.getNextTarEntry()) != null) {
                    String name = tarArchiveEntry.getName();
                    File tarFile = new File(parentFile, name + ".tmp");
                    logger.info("start to download file " + tarFile.getName());
                    if (tarFile.exists()) {
                        tarFile.delete();
                    }
                    BufferedOutputStream bos = null;
                    try {
                        bos = new BufferedOutputStream(new FileOutputStream(tarFile));
                        int read = -1;
                        byte[] buffer = new byte[1024];
                        while ((read = tais.read(buffer)) != -1) {
                            bos.write(buffer, 0, read);
                        }
                        logger.info("download file " + tarFile.getName() + " end!");
                        tarFile.renameTo(new File(parentFile, name));
                    } finally {
                        IOUtils.closeQuietly(bos);
                    }
                }
                tais.close();
            } else {
                File file = new File(parentFile, fileName + ".tmp");
                if (file.exists()) {
                    file.delete();
                }

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
                file.renameTo(new File(parentFile, fileName));
            }
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    public void execute(Runnable runnable) throws InterruptedException {
        taskQueue.put(runnable);
    }

    private class DownloadThread implements Runnable {

        @Override
        public void run() {
            while (running) {
                BinlogFile binlogFile = null;
                try {
                    binlogFile = downloadQueue.poll(5000, TimeUnit.MILLISECONDS);
                    if (binlogFile != null) {
                        int retry = 1;
                        while (true) {
                            try {
                                download(binlogFile);
                                break;
                            } catch (Throwable e) {
                                if (retry % 10 == 0) {
                                    retry = retry + 1;
                                    try {
                                        logger.warn("download failed + " + binlogFile.toString() + "], retry : "
                                                    + retry, e);
                                        // File errorFile = new File(destDir,
                                        // "error.txt");
                                        // FileWriter writer = new
                                        // FileWriter(errorFile);
                                        // writer.write(ExceptionUtils.getFullStackTrace(e));
                                        // writer.flush();
                                        // IOUtils.closeQuietly(writer);
                                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100 * retry));
                                    } catch (Throwable e1) {
                                        logger.error("write error failed", e1);
                                    }
                                } else {
                                    retry = retry + 1;
                                }
                            }
                        }
                    }

                    Runnable runnable = taskQueue.poll(5000, TimeUnit.MILLISECONDS);
                    if (runnable != null) {
                        runnable.run();
                    }
                } catch (Throwable e) {
                    logger.error("task process failed", e);
                }
            }

        }
    }
}

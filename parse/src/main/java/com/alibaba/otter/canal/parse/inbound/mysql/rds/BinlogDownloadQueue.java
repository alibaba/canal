package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author chengjin.lyf on 2018/8/7 下午3:10
 * @since 1.0.25
 */
public class BinlogDownloadQueue {

    private static final Logger logger = LoggerFactory.getLogger(BinlogDownloadQueue.class);
    private static final int      TIMEOUT             = 10000;

    private LinkedBlockingQueue<BinlogFile> downloadQueue = new LinkedBlockingQueue<BinlogFile>();
    private LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<Runnable>();
    private LinkedList<BinlogFile> binlogList;
    private final int batchSize;
    private Thread downloadThread;
    public boolean running = true;
    private final String destDir;
    private String hostId;
    private int currentSize;
    private String lastDownload;

    public BinlogDownloadQueue(List<BinlogFile> downloadQueue, int batchSize, String destDir) throws IOException {
        this.binlogList = new LinkedList(downloadQueue);
        this.batchSize = batchSize;
        this.destDir = destDir;
        this.currentSize = 0;
        prepareBinlogList();
        cleanDir();
    }

    private void prepareBinlogList(){
        for (BinlogFile binlog : this.binlogList) {
            String fileName = StringUtils.substringBetween(binlog.getDownloadLink(), "mysql-bin.", "?");
            binlog.setFileName(fileName);
        }
        Collections.sort(this.binlogList, new Comparator<BinlogFile>() {
            @Override
            public int compare(BinlogFile o1, BinlogFile o2) {
                return o1.getFileName().compareTo(o2.getFileName());
            }
        });
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
        downloadThread = new Thread(new DownloadThread());
        downloadThread.start();
    }


    public BinlogFile tryOne() throws IOException {
        BinlogFile binlogFile = binlogList.poll();
        download(binlogFile);
        hostId = binlogFile.getHostInstanceID();
        this.currentSize ++;
        return binlogFile;
    }

    public void notifyNotMatch(){
        this.currentSize --;
        filter(hostId);
    }

    private void filter(String hostInstanceId){
        Iterator<BinlogFile> it = binlogList.iterator();
        while (it.hasNext()){
            BinlogFile bf = it.next();
            if(bf.getHostInstanceID().equalsIgnoreCase(hostInstanceId)){
                it.remove();
            }else{
                hostId = bf.getHostInstanceID();
            }
        }
    }

    public boolean isLastFile(String fileName){
        String needCompareName = lastDownload;
        if (StringUtils.isNotEmpty(needCompareName) && StringUtils.endsWith(needCompareName, "tar")){
            needCompareName = needCompareName.substring(0, needCompareName.indexOf("."));
        }
        return fileName.equalsIgnoreCase(needCompareName) && binlogList.isEmpty();
    }

    public void prepare() throws InterruptedException {
        for (int i = this.currentSize; i < batchSize && !binlogList.isEmpty(); i++) {
            BinlogFile binlogFile = null;
            while (!binlogList.isEmpty()){
                binlogFile = binlogList.poll();
                if (!binlogFile.getHostInstanceID().equalsIgnoreCase(hostId)){
                    continue;
                }
                break;
            }
            if (binlogFile == null){
                break;
            }
            this.downloadQueue.put(binlogFile);
            this.lastDownload = "mysql-bin." + binlogFile.getFileName();
            this.currentSize ++;
        }
    }

    public void downOne(){
        this.currentSize --;
    }

    public void release(){
        running = false;
        this.currentSize = 0;
        binlogList.clear();
        downloadQueue.clear();
    }

    private void download(BinlogFile binlogFile) throws IOException {
        String downloadLink = binlogFile.getDownloadLink();
        String fileName = binlogFile.getFileName();
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
        saveFile(new File(destDir), "mysql-bin." + fileName, response);
    }

    private static void saveFile(File parentFile, String fileName, HttpResponse response) throws IOException {
        InputStream is = response.getEntity().getContent();
        long totalSize = Long.parseLong(response.getFirstHeader("Content-Length").getValue());
        if(response.getFirstHeader("Content-Disposition")!=null){
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
                try {
                    BinlogFile binlogFile = downloadQueue.poll(5000, TimeUnit.MILLISECONDS);
                    if (binlogFile != null){
                        download(binlogFile);
                    }
                    Runnable runnable = taskQueue.poll(5000, TimeUnit.MILLISECONDS);
                    if (runnable != null){
                        runnable.run();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }
}

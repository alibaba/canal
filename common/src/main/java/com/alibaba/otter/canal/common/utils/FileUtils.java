package com.alibaba.otter.canal.common.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static String readFileFromOffset(String filename, int l, String charset) {
        return readFileFromOffset(filename, l, charset, 4 * 1024 * 1024);
    }

    public static String readFileFromOffset(String filename, int l, String charset, int maxSize) {
        RandomAccessFile rf = null;
        StringBuilder res = new StringBuilder();
        try {
            rf = new RandomAccessFile(filename, "r");
            long fileLength = rf.length();
            long start = rf.getFilePointer();
            long readIndex = start + fileLength - 1;
            long minIndex = readIndex - maxSize;
            if (minIndex < 0) {
                minIndex = 0;
            }

            if (readIndex < 0) {
                readIndex = 0;
            }
            rf.seek(readIndex);
            int k = 0;
            int c = -1;
            String line = null;
            while (readIndex > start) {
                if (k == l) {
                    break;
                }
                c = rf.read();
                String readText = null;
                if (c == '\n' || c == '\r') {
                    line = rf.readLine();
                    if (line != null) {
                        readText = new String(line.getBytes(StandardCharsets.ISO_8859_1), charset);
                    } else {
                        if (k != 0) {
                            res.insert(0, "\n");
                        }
                        res.insert(0, "");
                        k++;
                    }
                    readIndex--;
                }
                readIndex--;
                if (readIndex < minIndex) {
                    break;
                } else {
                    rf.seek(readIndex);
                }

                if (readIndex == 0) {
                    readText = rf.readLine();
                }
                if (readText != null) {
                    if (k != 0) {
                        res.insert(0, "\n");
                    }
                    res.insert(0, readText);
                    k++;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (rf != null) {
                try {
                    rf.close();
                } catch (IOException e) {
                    // ignore
                }
                rf = null;
            }
        }

        return res.toString();
    }

    /**
     * 校验自定义的文件名，是否在允许的基目录范围内，如何合法就返回全路径，否则就直接报错
     *
     * @param baseDir
     * @param destination
     * @return
     */
    public static String validateFileName(String baseDir, String destination) {
        try {
            // 验证 destination 是否在允许的基目录范围内
            String basePath = new File(baseDir).getCanonicalPath();
            String fullPath = new File(basePath, destination).getCanonicalPath();

            // 检查 fullPath 是否以 basePath 开头
            if (!fullPath.startsWith(basePath + File.separator)) {
                throw new IllegalArgumentException("Invalid destination path");
            }

            return fullPath;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file", e);
        }
    }

    public static void main(String[] args) throws IOException {
        String fullPath = validateFileName("/tmp/", "1.txt");
        System.out.println(fullPath);
        System.out.println(org.apache.commons.io.FileUtils.readLines(new File(fullPath)));

        fullPath = validateFileName("/tmp/", "test");
        fullPath = validateFileName(fullPath,"1.txt");
        System.out.println(fullPath);
        System.out.println(org.apache.commons.io.FileUtils.readLines(new File(fullPath)));


        fullPath = validateFileName("/tmp/", "../etc/hosts");
        System.out.println(fullPath);
        System.out.println(org.apache.commons.io.FileUtils.readLines(new File(fullPath)));
    }
}

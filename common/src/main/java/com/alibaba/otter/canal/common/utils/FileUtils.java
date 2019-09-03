package com.alibaba.otter.canal.common.utils;

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

    public static void main(String[] args) {
        String res = readFileFromOffset("test2.txt", 2, "UTF-8");
        System.out.println(res);
    }
}

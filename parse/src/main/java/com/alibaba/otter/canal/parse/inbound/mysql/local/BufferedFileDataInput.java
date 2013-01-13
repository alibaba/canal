package com.alibaba.otter.canal.parse.inbound.mysql.local;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jianghang 2012-7-7 下午03:10:47
 * @version 1.0.0
 */
public class BufferedFileDataInput {

    private static final Logger logger = LoggerFactory.getLogger(BufferedFileDataInput.class);
    // Read parameters.
    private File                file;
    private int                 size;

    // Variables to control reading.
    private FileInputStream     fileInput;
    private BufferedInputStream bufferedInput;
    private DataInputStream     dataInput;
    private long                offset;
    private FileChannel         fileChannel;

    public BufferedFileDataInput(File file, int size) throws FileNotFoundException, IOException, InterruptedException{
        this.file = file;
        this.size = size;
    }

    public BufferedFileDataInput(File file) throws FileNotFoundException, IOException, InterruptedException{
        this(file, 1024);
    }

    public long available() throws IOException {
        return fileChannel.size() - offset;
    }

    public long skip(long bytes) throws IOException {
        long bytesSkipped = bufferedInput.skip(bytes);
        offset += bytesSkipped;
        return bytesSkipped;
    }

    public void seek(long seekBytes) throws FileNotFoundException, IOException, InterruptedException {
        fileInput = new FileInputStream(file);
        fileChannel = fileInput.getChannel();

        try {
            fileChannel.position(seekBytes);
        } catch (ClosedByInterruptException e) {
            throw new InterruptedException();
        }
        bufferedInput = new BufferedInputStream(fileInput, size);
        dataInput = new DataInputStream(bufferedInput);
        offset = seekBytes;
    }

    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    public void readFully(byte[] bytes, int start, int len) throws IOException {
        dataInput.readFully(bytes, start, len);
        offset += len;
    }

    public void close() {
        try {
            if (fileChannel != null) {
                fileChannel.close();
                fileInput.close();
            }
        } catch (IOException e) {
            logger.warn("Unable to close buffered file reader: file=" + file.getName() + " exception=" + e.getMessage());
        }

        fileChannel = null;
        fileInput = null;
        bufferedInput = null;
        dataInput = null;
        offset = -1;
    }

}

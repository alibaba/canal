package com.alibaba.otter.canal.parse.inbound.emulator.oracle.data.impl;

import java.util.Arrays;

import com.alibaba.otter.canal.parse.inbound.emulator.oracle.data.BinLogFile;

/**
 * byte array binlog file file ,To change this template use File | Settings | File Templates.
 * 
 * @author: yuanzu Date: 12-9-24 Time: 下午4:24
 */
public class ByteArrayBinLogFile implements BinLogFile {

    private final String filename;
    private final byte[] data;

    public ByteArrayBinLogFile(String filename, byte[] data){
        this.filename = filename;
        this.data = data;
    }

    @Override
    public String getFilename() {
        return this.filename;
    }

    @Override
    public byte[] getData(int offset, int len) {
        return Arrays.copyOfRange(data, offset, offset + len);
    }

    @Override
    public int getLength() {
        return this.data.length;
    }
}

package com.alibaba.otter.canal.parse.inbound.emulator.oracle.data.impl;

import com.alibaba.otter.canal.parse.inbound.emulator.oracle.data.BinLogFile;

/**
 * local file bin log file, To change this template use File | Settings | File Templates.
 * 
 * @author: yuanzu Date: 12-9-24 Time: 下午4:32
 */
public class LocalBinLogFile implements BinLogFile {

    @Override
    public String getFilename() {
        return null; // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte[] getData(int offset, int len) {
        return new byte[0]; // To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getLength() {
        return 0; // To change body of implemented methods use File | Settings | File Templates.
    }
}

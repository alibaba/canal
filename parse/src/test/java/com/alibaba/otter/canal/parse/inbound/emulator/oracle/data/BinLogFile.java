package com.alibaba.otter.canal.parse.inbound.emulator.oracle.data;

/**
 * Binary log file,To change this template use File | Settings | File Templates.
 * 
 * @author: yuanzu Date: 12-9-24 Time: 下午3:45
 */
public interface BinLogFile {

    public String getFilename();

    public byte[] getData(int offset, int len);

    public int getLength();
}

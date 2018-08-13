package com.alibaba.otter.canal.parse.inbound.mysql.rds.data;

import java.util.List;

/**
 * @author chengjin.lyf on 2018/8/7 下午2:26
 * @since 1.0.25
 */
public class RdsItem {

    private List<BinlogFile> BinLogFile;

    public List<BinlogFile> getBinLogFile() {
        return BinLogFile;
    }

    public void setBinLogFile(List<BinlogFile> binLogFile) {
        BinLogFile = binLogFile;
    }

    @Override
    public String toString() {
        return "RdsItem [BinLogFile=" + BinLogFile + "]";
    }

}

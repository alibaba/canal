package com.alibaba.otter.canal.parse.inbound.mysql;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogOpenApi;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.RdsBackupPolicy;

/**
 * @author agapple 2017年10月15日 下午2:14:34
 * @since 1.0.25
 */
@Ignore
public class RdsBinlogOpenApiTest {

    @Test
    public void testSimple() throws Throwable {
        Date startTime = DateUtils.parseDate("2018-08-10 12:00:00", new String[] { "yyyy-MM-dd HH:mm:ss" });
        Date endTime = DateUtils.parseDate("2018-08-11 12:00:00", new String[] { "yyyy-MM-dd HH:mm:ss" });
        String url = "https://rds.aliyuncs.com/";
        String ak = "";
        String sk = "";
        String dbInstanceId = "";

        RdsBackupPolicy backupPolicy = RdsBinlogOpenApi.queryBinlogBackupPolicy(url, ak, sk, dbInstanceId);
        System.out.println(backupPolicy);

        List<BinlogFile> binlogFiles = RdsBinlogOpenApi.listBinlogFiles(url, ak, sk, dbInstanceId, startTime, endTime);
        System.out.println(binlogFiles);
    }
}

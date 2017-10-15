package com.alibaba.otter.canal.parse.inbound.mysql;

import java.io.File;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;

import com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogOpenApi;

/**
 * @author agapple 2017年10月15日 下午2:14:34
 * @since 1.0.25
 */
public class RdsBinlogOpenApiTest {

    public void testSimple() throws Throwable {
        Date startTime = DateUtils.parseDate("2017-10-13 20:56:58", new String[] { "yyyy-MM-dd HH:mm:ss" });
        Date endTime = DateUtils.parseDate("2017-10-14 02:57:59", new String[] { "yyyy-MM-dd HH:mm:ss" });
        RdsBinlogOpenApi.downloadBinlogFiles("https://rds.aliyuncs.com/",
            "",
            "",
            "rm-bp180v4mfjnm157es",
            startTime,
            endTime,
            new File("/tmp/binlog/"));
    }
}

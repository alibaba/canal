package com.alibaba.otter.canal.parse.inbound.mysql.rds;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.BinlogFile;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.DescribeBinlogFileResult;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.RdsBackupPolicy;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.RdsItem;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.request.DescribeBackupPolicyRequest;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.request.DescribeBinlogFilesRequest;

/**
 * @author agapple 2017年10月14日 下午1:53:52
 * @since 1.0.25
 */
public class RdsBinlogOpenApi {

    protected static final Logger logger = LoggerFactory.getLogger(RdsBinlogOpenApi.class);

    public static List<BinlogFile> listBinlogFiles(String url, String ak, String sk, String dbInstanceId,
                                                   Date startTime, Date endTime) {
        DescribeBinlogFilesRequest request = new DescribeBinlogFilesRequest();
        if (StringUtils.isNotEmpty(url)) {
            try {
                URI uri = new URI(url);
                request.setEndPoint(uri.getHost());
            } catch (URISyntaxException e) {
                logger.error("resolve url host failed, will use default rds endpoint!");
            }
        }
        request.setStartDate(startTime);
        request.setEndDate(endTime);
        request.setPageNumber(1);
        request.setPageSize(100);
        request.setRdsInstanceId(dbInstanceId);
        request.setAccessKeyId(ak);
        request.setAccessKeySecret(sk);
        DescribeBinlogFileResult result = null;
        int retryTime = 3;
        while (true) {
            try {
                result = request.doAction();
                break;
            } catch (Exception e) {
                if (retryTime-- <= 0) {
                    throw new RuntimeException(e);
                }
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e1) {
                }
            }
        }
        if (result == null) {
            return Collections.EMPTY_LIST;
        }
        RdsItem rdsItem = result.getItems();
        if (rdsItem != null) {
            return rdsItem.getBinLogFile();
        }
        return Collections.EMPTY_LIST;
    }

    public static RdsBackupPolicy queryBinlogBackupPolicy(String url, String ak, String sk, String dbInstanceId) {
        DescribeBackupPolicyRequest request = new DescribeBackupPolicyRequest();
        if (StringUtils.isNotEmpty(url)) {
            try {
                URI uri = new URI(url);
                request.setEndPoint(uri.getHost());
            } catch (URISyntaxException e) {
                logger.error("resolve url host failed, will use default rds endpoint!");
            }
        }
        request.setRdsInstanceId(dbInstanceId);
        request.setAccessKeyId(ak);
        request.setAccessKeySecret(sk);
        int retryTime = 3;
        while (true) {
            try {
                return request.doAction();
            } catch (Exception e) {
                if (retryTime-- <= 0) {
                    throw new RuntimeException(e);
                }
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e1) {
                }
            }
        }
    }
}

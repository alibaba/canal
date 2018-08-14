package com.alibaba.otter.canal.parse.inbound.mysql.rds.request;

import java.util.Date;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.DescribeBinlogFileResult;

/**
 * @author chengjin.lyf on 2018/8/7 下午3:41
 * @since 1.0.25
 */
public class DescribeBinlogFilesRequest extends AbstractRequest<DescribeBinlogFileResult> {

    public DescribeBinlogFilesRequest(){
        setVersion("2014-08-15");
        putQueryString("Action", "DescribeBinlogFiles");
    }

    public void setRdsInstanceId(String rdsInstanceId) {
        putQueryString("DBInstanceId", rdsInstanceId);
    }

    public void setPageSize(int pageSize) {
        putQueryString("PageSize", String.valueOf(pageSize));
    }

    public void setPageNumber(int pageNumber) {
        putQueryString("PageNumber", String.valueOf(pageNumber));
    }

    public void setStartDate(Date startDate) {
        putQueryString("StartTime", formatUTCTZ(startDate));
    }

    public void setEndDate(Date endDate) {
        putQueryString("EndTime", formatUTCTZ(endDate));
    }

    public void setResourceOwnerId(Long resourceOwnerId) {
        putQueryString("ResourceOwnerId", String.valueOf(resourceOwnerId));
    }

    @Override
    protected DescribeBinlogFileResult processResult(HttpResponse response) throws Exception {
        String result = EntityUtils.toString(response.getEntity());
        DescribeBinlogFileResult describeBinlogFileResult = JSONObject.parseObject(result,
            new TypeReference<DescribeBinlogFileResult>() {
            });
        return describeBinlogFileResult;
    }
}

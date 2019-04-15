package com.alibaba.otter.canal.parse.inbound.mysql.rds.request;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.data.RdsBackupPolicy;

/**
 * rds 备份策略查询
 * 
 * @author chengjin.lyf on 2018/8/7 下午3:41
 * @since 1.0.25
 */
public class DescribeBackupPolicyRequest extends AbstractRequest<RdsBackupPolicy> {

    public DescribeBackupPolicyRequest(){
        setVersion("2014-08-15");
        putQueryString("Action", "DescribeBackupPolicy");

    }

    public void setRdsInstanceId(String rdsInstanceId) {
        putQueryString("DBInstanceId", rdsInstanceId);
    }

    @Override
    protected RdsBackupPolicy processResult(HttpResponse response) throws Exception {
        String result = EntityUtils.toString(response.getEntity());
        JSONObject jsonObj = JSON.parseObject(result);
        RdsBackupPolicy policy = new RdsBackupPolicy();
        policy.setBackupRetentionPeriod(jsonObj.getString("BackupRetentionPeriod"));
        policy.setBackupLog(jsonObj.getString("BackupLog").equalsIgnoreCase("Enable"));
        policy.setLogBackupRetentionPeriod(jsonObj.getIntValue("LogBackupRetentionPeriod"));
        policy.setPreferredBackupPeriod(jsonObj.getString("PreferredBackupPeriod"));
        policy.setPreferredBackupTime(jsonObj.getString("PreferredBackupTime"));
        return policy;
    }
}

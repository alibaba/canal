package com.alibaba.otter.canal.client.adapter.tablestore;

import com.alibaba.otter.canal.client.adapter.support.Dml;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * @Description
 * @Author yun.zhang
 * @Date 2022/12/15 15:43
 * @Version 1.0
 * @ClassName TablestoreSyncTest.class
 */
@Ignore
public class TablestoreSyncTest {


    private TablestoreAdapter tablestoreAdapter;

    @Before
    public void init() {
        tablestoreAdapter = Common.init();
    }

    @Test
    public void syncTest() {
        Dml dml = new Dml();
        dml.setDestination("tablestore");
        dml.setGroupId("g1");
        dml.setTs(new Date().getTime());
        dml.setType("INSERT");
        dml.setDatabase("mgs_im_message");
        dml.setTable("im_cmn_msg_send");
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> data = new LinkedHashMap<>();
        dataList.add(data);
        data.put("msg_id", "msg_20221216zy01");
        data.put("community_id", 419880);
        data.put("channel_id", 888889);
        data.put("type", 5L);
        data.put("seq", "1664415668888");
        data.put("act_type", "0");
        data.put("action", "{\"msgId\":\"3GU6TVUF22332N22EU6E\"}");
        data.put("cipher_content", "0xD71CB604B920E880C6872334C470319D410595DEDC18A9B45D77041707ACF7EA");
        data.put("act_type", "0");
        data.put("mark", "0");
        data.put("operate_uid", "0");

        dml.setData(dataList);

        tablestoreAdapter.sync(Collections.singletonList(dml));
    }


    @Test
    public void etlTest() {
        tablestoreAdapter.etl("constant_support_demo.yml", new ArrayList<>());
    }
}

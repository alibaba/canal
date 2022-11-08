package com.alibaba.otter.canal.parse.driver.mysql;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.alibaba.otter.canal.parse.driver.mysql.packets.UUIDSet;

/**
 * Created by hiwjd on 2018/4/26. hiwjd0@gmail.com
 */
public class UUIDSetTest {

    @Test
    public void testToString() {
        Map<String, String> cases = new HashMap<>(4);
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1", "726757ad-4455-11e8-ae04-0242ac110002:1");
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1-3", "726757ad-4455-11e8-ae04-0242ac110002:1-3");
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1-3:4-6", "726757ad-4455-11e8-ae04-0242ac110002:1-6");
        cases.put("726757ad-4455-11e8-ae04-0242ac110002:1-3:5-7", "726757ad-4455-11e8-ae04-0242ac110002:1-3:5-7");

        for (Map.Entry<String, String> entry : cases.entrySet()) {
            String expected = entry.getValue();
            assertEquals(expected, UUIDSet.parse(entry.getKey()).toString());
        }
    }
}

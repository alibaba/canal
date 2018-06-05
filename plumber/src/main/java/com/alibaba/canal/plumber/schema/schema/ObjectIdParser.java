package com.alibaba.canal.plumber.schema.schema;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

/**
 * objectId解析器
 * @author dsqin
 * @date 2018/6/5
 */
public class ObjectIdParser {

    private static final String OBJECT_KEY = "$oid";

    public static String valueParse(String value)
    {
        if (StringUtils.isBlank(value)) {
            return value;
        }

        JSONObject jsonObject = JSON.parseObject(value);
        if ((null == jsonObject) || (jsonObject.isEmpty())) {
            return "";
        }
        return jsonObject.get(OBJECT_KEY).toString();
    }
}

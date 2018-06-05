package com.alibaba.canal.plumber.schema;


import com.alibaba.canal.plumber.model.EventData;
import com.alibaba.canal.plumber.model.EventType;
import com.alibaba.canal.plumber.schema.schema.ObjectIdParser;
import com.alibaba.canal.plumber.schema.schema.SchemaFactory;
import com.alibaba.canal.plumber.schema.schema.Schemas;
import com.alibaba.canal.plumber.schema.schema.define.EmbedSchema;
import com.alibaba.canal.plumber.schema.schema.define.EventSchema;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public class TransformService implements ITransform
{
    private static final Logger logger = LoggerFactory.getLogger(TransformService.class);
    private SchemaFactory schemaFactory;

    public TransformService() {}

    public TransformService(SchemaFactory schemaFactory)
    {
        this.schemaFactory = schemaFactory;
    }

    public boolean transform(EventData eventData)
    {
        String schema = eventData.getSchemaName();
        EventType eventType = eventData.getEventType();
        String body = eventData.getBodyStringUtf8();

        JSONObject jsonObject = JSON.parseObject(body);
        if (StringUtils.isBlank(schema))
        {
            logger.info("****Plumber transform������:���������������,schema������������,������������:{},������������:{}****",
                    Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()));
            return false;
        }
        if ((null == jsonObject) || (jsonObject.isEmpty()))
        {
            logger.info("****Plumber transform������:���������������,json������������������,������������:{},������������:{},������������:{}****", new Object[] {
                    Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()), body });
            return false;
        }
        Schemas schemas = this.schemaFactory.getSchema(schema);
        if (null == schemas)
        {
            logger.info("****Plumber tansform������:���������������,schema���������,������������:{},������������:{},������������:{}****", new Object[] {
                    Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()), body });
            return false;
        }
        Set<String> keys = jsonObject.keySet();

        EventSchema eventSchema = schemas.getEvent();

        Map<String, String> aliasMap = schemas.getAliases();
        Set<String> includes = null;
        if (null != schemas.getIncludes()) {
            includes = schemas.getIncludes().getFieldValues();
        }
        Set<String> exludes = null;
        if (null != schemas.getExcludes()) {
            exludes = schemas.getExcludes().getFieldValues();
        }
        JSONObject transformedJsonObj = new JSONObject();

        String pkName = StringUtils.defaultIfBlank(eventSchema.pk, "_id");
        for (String key : keys)
        {
            Object value = jsonObject.get(key);
            if ((null == value) || (StringUtils.isBlank(value.toString())))
            {
                logger.info("****Plumber transform������--������������������,������:{},������������:{},������������:{}****",
                        key, eventData.getSequenceId(), eventData.getStage());
            } else {
                boolean isPk = key.equals(pkName);
                if (isPk) {
                    value = ObjectIdParser.valueParse(value.toString());
                }
                if ((null != includes) && (!includes.contains(key)) && (!isPk))
                {
                    logger.info("****Plumber transform������--������������include���,������:{},������������:{},������������:{}****", key,
                            eventData.getSequenceId(), eventData.getStage());
                }
                else if ((null != exludes) && (exludes.contains(key)) && (!isPk))
                {
                    logger.info("****Plumber transform������--���������exclude���,������:{},������������:{},������������:{}****", new Object[] { key,
                            Long.valueOf(eventData.getSequenceId()), Integer.valueOf(eventData.getStage()) });
                }
                else
                {
                    String transformedKey;
                    if ((null != aliasMap) && (aliasMap.containsKey(key))) {
                        transformedKey = aliasMap.get(key);
                    } else {
                        transformedKey = key;
                    }
                    transformedJsonObj.put(transformedKey, value);
                }
            }
        }
        Map<String, String> params = Maps.newHashMap();
        params.put("index", eventSchema.index);
        params.put("type", eventSchema.type);
        params.put("pk", pkName);

        EmbedSchema embedSchema = schemas.getEmbed();
        if ((null != embedSchema) && (StringUtils.isNotBlank(embedSchema.value)))
        {
            String pkValue = transformedJsonObj.getString(pkName);
            transformedJsonObj.remove(pkName);

            JSONObject embedJsonObject = new JSONObject();
            embedJsonObject.put(embedSchema.value, transformedJsonObj.toJSONString());
            embedJsonObject.put(pkName, pkValue);
            eventData.setBody(embedJsonObject.toJSONString());

            ((Map)params).put("embed", embedSchema.value);
        }
        else
        {
            eventData.setBody(transformedJsonObj.toJSONString());
        }
        eventData.setHeaders((Map)params);

        return true;
    }
}

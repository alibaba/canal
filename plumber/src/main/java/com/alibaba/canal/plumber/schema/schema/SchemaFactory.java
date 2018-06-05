package com.alibaba.canal.plumber.schema.schema;

import java.util.Map;

/**
 * schema工厂类
 * @author dsqin
 * @date 2018/6/5
 */
public class SchemaFactory {

    private Map<String, Schemas> schemas;

    public SchemaFactory()
    {
        init();
    }

    public void init()
    {
        this.schemas = SchemaXmlParser.parse();
    }

    public Schemas getSchema(String name)
    {
        if (null == this.schemas) {
            return null;
        }
        return (Schemas)this.schemas.get(name);
    }
}

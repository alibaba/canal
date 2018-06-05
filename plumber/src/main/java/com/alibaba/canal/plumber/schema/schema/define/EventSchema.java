package com.alibaba.canal.plumber.schema.schema.define;

import com.alibaba.canal.plumber.schema.schema.SchemaValid;
import java.lang.reflect.Field;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Attribute;

/**
 * event头信息
 * @author dsqin
 * @date 2018/6/5
 */
public class EventSchema implements SchemaValid {
    public String id;
    public String index;
    public String type;
    public String pk;
    public String clz;

    public EventSchema(List<Attribute> attributes) {
        for (Attribute attribute : attributes) {
            String name = attribute.getName();
            try {
                Field field = getClass().getDeclaredField(name);
                field.setAccessible(true);
                field.set(this, attribute.getValue());
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (NoSuchFieldException localNoSuchFieldException) {
            }
        }
    }

    public boolean isValid() {
        return StringUtils.isNotBlank(this.id);
    }
}
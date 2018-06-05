package com.alibaba.canal.plumber.schema.schema.define;

import com.alibaba.canal.plumber.schema.schema.SchemaValid;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Element;

/**
 * 别名 alias
 * @author dsqin
 * @date 2018/6/5
 */
public class AliasSchema implements SchemaValid {

    private static final String FROM_ATTRIBUTE = "from";
    private static final String TO_ATTRIBUTE = "to";
    String from;
    String to;

    public AliasSchema(Element element)
    {
        this.from = element.attribute(FROM_ATTRIBUTE).getValue();

        this.to = element.attribute(TO_ATTRIBUTE).getValue();
    }

    public boolean isValid()
    {
        return (StringUtils.isNotBlank(this.from)) && (StringUtils.isNotBlank(this.to));
    }

    public void add(Map<String, String> map)
    {
        map.put(this.from, this.to);
    }
}

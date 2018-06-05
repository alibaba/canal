package com.alibaba.canal.plumber.schema.schema.define;

import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Element;

import java.util.Arrays;
import java.util.Set;

/**
 * abstract值对象schema
 * @author dsqin
 * @date 2018/6/5
 */
public abstract class AbstractValueSchema {

    private static final String SEPERATOR = ",";
    public String value;

    public AbstractValueSchema(Element element)
    {
        if (element.attributes().isEmpty()) {
            return;
        }
        this.value = element.attribute(0).getValue();
    }

    public Set<String> getFieldValues()
    {
        String[] attributeArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(this.value, ",");
        if (ArrayUtils.isEmpty(attributeArray)) {
            return null;
        }
        return Sets.newHashSet(Arrays.asList(attributeArray));
    }
}

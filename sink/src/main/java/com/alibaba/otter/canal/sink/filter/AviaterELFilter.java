package com.alibaba.otter.canal.sink.filter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.CanalEventFilter;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.googlecode.aviator.AviatorEvaluator;

/**
 * 基于aviater el表达式的匹配过滤
 * 
 * @author jianghang 2012-7-23 上午10:46:32
 */
public class AviaterELFilter implements CanalEventFilter<CanalEntry.Entry> {

    public static final String ROOT_KEY = "entry";
    private String             expression;

    public AviaterELFilter(String expression){
        this.expression = expression;
    }

    public boolean filter(CanalEntry.Entry entry) throws CanalSinkException {
        if (StringUtils.isEmpty(expression)) {
            return true;
        }

        Map<String, Object> env = new HashMap<String, Object>();
        env.put(ROOT_KEY, entry);
        return (Boolean) AviatorEvaluator.execute(expression, env);
    }

}

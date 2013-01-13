package com.alibaba.otter.canal.sink.filter;

import java.util.Map;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;

/**
 * 提供aviator regex的代码扩展
 * 
 * @author jianghang 2012-7-23 上午10:29:23
 */
public class RegexFunction extends AbstractFunction {

    private Map<String, Pattern> patterns = null;

    public RegexFunction(){
        patterns = new MapMaker().softValues().makeComputingMap(new Function<String, Pattern>() {

            public Pattern apply(String pattern) {
                try {
                    PatternCompiler pc = new Perl5Compiler();
                    return pc.compile(pattern, Perl5Compiler.CASE_INSENSITIVE_MASK | Perl5Compiler.READ_ONLY_MASK);
                } catch (MalformedPatternException e) {
                    throw new CanalSinkException(e);
                }
            }
        });
    }

    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        String pattern = FunctionUtils.getStringValue(arg1, env);
        String text = FunctionUtils.getStringValue(arg2, env);
        Perl5Matcher matcher = new Perl5Matcher();
        boolean isMatch = matcher.matches(text, patterns.get(pattern));
        return AviatorBoolean.valueOf(isMatch);
    }

    public String getName() {
        return "regex";
    }

}

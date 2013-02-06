package com.alibaba.otter.canal.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Otter项目内部使用的ToStringStyle
 * 
 * <pre>
 * 默认Style输出格式：
 * Person[name=John Doe,age=33,smoker=false ,time=2010-04-01 00:00:00]
 * </pre>
 * 
 * @author jianghang 2010-6-18 上午11:35:27
 */
public class CanalToStringStyle extends ToStringStyle {

    private static final long         serialVersionUID = -6568177374288222145L;

    private static final String       DEFAULT_TIME     = "yyyy-MM-dd HH:mm:ss";
    private static final String       DEFAULT_DAY      = "yyyy-MM-dd";

    /**
     * <pre>
     * 输出格式：
     * Person[name=John Doe,age=33,smoker=false ,time=2010-04-01 00:00:00]
     * </pre>
     */
    public static final ToStringStyle TIME_STYLE       = new OtterDateStyle(DEFAULT_TIME);

    /**
     * <pre>
     * 输出格式：
     * Person[name=John Doe,age=33,smoker=false ,day=2010-04-01]
     * </pre>
     */
    public static final ToStringStyle DAY_STYLE        = new OtterDateStyle(DEFAULT_DAY);

    /**
     * <pre>
     * 输出格式：
     * Person[name=John Doe,age=33,smoker=false ,time=2010-04-01 00:00:00]
     * </pre>
     */
    public static final ToStringStyle DEFAULT_STYLE    = CanalToStringStyle.TIME_STYLE;

    // =========================== 自定义style =============================

    /**
     * 支持日期格式化的ToStringStyle
     * 
     * @author li.jinl
     */
    private static class OtterDateStyle extends ToStringStyle {

        private static final long serialVersionUID = 5208917932254652886L;

        // 日期format格式
        private String            pattern;

        public OtterDateStyle(String pattern){
            super();
            this.setUseShortClassName(true);
            this.setUseIdentityHashCode(false);
            // 设置日期format格式
            this.pattern = pattern;
        }

        protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
            // 增加自定义的date对象处理
            if (value instanceof Date) {
                value = new SimpleDateFormat(pattern).format(value);
            }
            // 后续可以增加其他自定义对象处理
            buffer.append(value);
        }
    }
}

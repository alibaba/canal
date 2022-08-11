package com.alibaba.otter.canal.connector.core.util;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DateUtil {

    private static DateTimeZone dateTimeZone;

    static {
        dateTimeZone = DateTimeZone.forID(TimeZone.LOCATION_TIME_ZONE);
    }

    /**
     * 通用日期时间字符解析
     *
     * @param datetimeStr 日期时间字符串
     * @return Date
     */
    public static Date parseDate(String datetimeStr) {
        if (StringUtils.isEmpty(datetimeStr)) {
            return null;
        }
        datetimeStr = datetimeStr.trim().replace('/', '-');
        if (datetimeStr.contains("-")) {
            if (datetimeStr.contains(":")) {
                datetimeStr = datetimeStr.replace(" ", "T");
            }
        } else if (datetimeStr.contains(":")) {
            datetimeStr = "T" + datetimeStr;
        } else {
            if (datetimeStr.length() == 8) {
                String year = datetimeStr.substring(0, 4);
                String month = datetimeStr.substring(4, 6);
                String day = datetimeStr.substring(6, 8);
                datetimeStr = year + "-" + month + "-" + day;
            }
        }

        DateTime dateTime = new DateTime(datetimeStr, dateTimeZone);

        return dateTime.toDate();
    }
}

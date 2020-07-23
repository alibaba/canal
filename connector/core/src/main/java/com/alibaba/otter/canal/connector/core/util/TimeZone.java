package com.alibaba.otter.canal.connector.core.util;

public class TimeZone {

    public final static String LOCATION_TIME_ZONE; // 当前时区

    static {
        java.util.TimeZone localTimeZone = java.util.TimeZone.getDefault();
        int rawOffset = localTimeZone.getRawOffset();
        String symbol = "+";
        if (rawOffset < 0) {
            symbol = "-";
        }
        rawOffset = Math.abs(rawOffset);
        int offsetHour = rawOffset / 3600000;
        int offsetMinute = rawOffset % 3600000 / 60000;
        String hour = String.format("%1$02d", offsetHour);
        String minute = String.format("%1$02d", offsetMinute);
        LOCATION_TIME_ZONE = symbol + hour + ":" + minute;
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("GMT" + LOCATION_TIME_ZONE));
    }
}

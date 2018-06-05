package com.alibaba.otter.canal.parse.inbound.mongo.meta;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.BSONTimestamp;

/**
 * bson时间戳工具类
 * @author dsqin
 * @date 2018/5/25
 */
public class BSONTimestampUtil {


    /**
     * bson时间戳转化为int
     * @param timestamp
     * @return
     */
    public static long encode(BSONTimestamp timestamp) {

        int time = timestamp.getTime();
        int incr = timestamp.getInc();

        String incrString = String.valueOf(incr);

        String encodeString = StringUtils.join(time, incr, incrString.length());

        return NumberUtils.toLong(encodeString);
    }

    /**
     *  int转化为bson时间戳
     * @return
     */
    public static BSONTimestamp decode(long val) {
        String timestampString = String.valueOf(val);

        int totalDigit = timestampString.length();

        String mantissa = timestampString.substring(totalDigit - 1);

        int mantissaInt = NumberUtils.toInt(mantissa);

        String timeString = timestampString.substring(0, totalDigit - mantissaInt - 1);

        String incrString = timestampString.substring(totalDigit - mantissaInt - 1, totalDigit-1);

        return new BSONTimestamp(NumberUtils.toInt(timeString), NumberUtils.toInt(incrString));
    }

    public static void main(String[] args) {
        BSONTimestamp timestamp = new BSONTimestamp(1518323462, 1823);

        long val = encode(timestamp);

        BSONTimestamp timestamp1 = decode(val);

        System.out.println(timestamp.compareTo(timestamp1));
    }
}

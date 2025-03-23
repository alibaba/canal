package com.alibaba.otter.canal.connector.core.util;


import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Ignore
public class DateUtilTest {
    @Test
    public void testDateNewerThan1582() {
        List<String> testCases = Arrays.asList("2020-08-10", "1970-1-1", "4000-12-23");
        List<String> expected = Arrays.asList("Mon Aug 10 00:00:00 GMT+08:00 2020", "Thu Jan 01 00:00:00 GMT+08:00 1970", "Sat Dec 23 00:00:00 GMT+08:00 4000");
        for (int i = 0; i < testCases.size(); i++) {
            Assert.assertEquals(expected.get(i), Objects.requireNonNull(DateUtil.parseDate(testCases.get(i))).toString());
        }
    }

    @Test
    public void testDateOlderThan1582() {
        List<String> testCases = Arrays.asList("1582-10-4", "1000-10-10", "1200-2-29");
        List<String> expected = Arrays.asList("Thu Oct 04 00:00:00 GMT+08:00 1582", "Thu Oct 10 00:00:00 GMT+08:00 1000", "Tue Feb 29 00:00:00 GMT+08:00 1200");
        for (int i = 0; i < testCases.size(); i++) {
            Assert.assertEquals(expected.get(i), Objects.requireNonNull(DateUtil.parseDate(testCases.get(i))).toString());
        }
    }

    @Test
    public void testDateIn1582() {
        List<String> testCases = Arrays.asList("1582-10-5", "1582-10-14", "1582-10-10", "1582-10-15");
        List<String> expected = Arrays.asList("Fri Oct 15 00:00:00 GMT+08:00 1582", "Sun Oct 24 00:00:00 GMT+08:00 1582", "Wed Oct 20 00:00:00 GMT+08:00 1582", "Fri Oct 15 00:00:00 GMT+08:00 1582");
        for (int i = 0; i < testCases.size(); i++) {
            Assert.assertEquals(expected.get(i), Objects.requireNonNull(DateUtil.parseDate(testCases.get(i))).toString());
        }
    }

    @Test
    public void testDateIn1582CornerCase() {
        List<String> testCases = Arrays.asList("1582-10-4 23:59", "1582-10-5 0:0", "1582-10-14 23:59", "1582-10-15 0:0");
        List<String> expected = Arrays.asList("Thu Oct 04 23:59:00 GMT+08:00 1582", "Fri Oct 15 00:00:00 GMT+08:00 1582", "Sun Oct 24 23:59:00 GMT+08:00 1582", "Fri Oct 15 00:00:00 GMT+08:00 1582");
        for (int i = 0; i < testCases.size(); i++) {
            Assert.assertEquals(expected.get(i), Objects.requireNonNull(DateUtil.parseDate(testCases.get(i))).toString());
        }
    }

}

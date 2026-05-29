package com.alibaba.otter.canal.filter;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.filter.aviater.AviaterELFilter;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.filter.aviater.AviaterSimpleFilter;
import com.alibaba.otter.canal.protocol.CanalEntry;

public class AviaterFilterTest {

    @Test
    public void test_simple() {
        AviaterSimpleFilter filter = new AviaterSimpleFilter("s1.t1,s2.t2");
        boolean result = filter.filter("s1.t1");
        Assert.assertEquals(true, result);

        result = filter.filter("s1.t2");
        Assert.assertEquals(false, result);

        result = filter.filter("");
        Assert.assertEquals(true, result);

        result = filter.filter("s1.t1,s2.t2");
        Assert.assertEquals(false, result);

        result = filter.filter("s2.t2");
        Assert.assertEquals(true, result);
    }

    @Test
    public void test_regex() {
        AviaterRegexFilter filter = new AviaterRegexFilter("s1\\..*,s2\\..*");
        boolean result = filter.filter("s1.t1");
        Assert.assertEquals(true, result);

        result = filter.filter("s1.t2");
        Assert.assertEquals(true, result);

        result = filter.filter("");
        Assert.assertEquals(true, result);

        result = filter.filter("s12.t1");
        Assert.assertEquals(false, result);

        result = filter.filter("s2.t2");
        Assert.assertEquals(true, result);

        result = filter.filter("s3.t2");
        Assert.assertEquals(false, result);

        result = filter.filter("S1.S2");
        Assert.assertEquals(true, result);

        result = filter.filter("S2.S1");
        Assert.assertEquals(true, result);

        AviaterRegexFilter filter2 = new AviaterRegexFilter("s1\\..*,s2.t1");

        result = filter2.filter("s1.t1");
        Assert.assertEquals(true, result);

        result = filter2.filter("s1.t2");
        Assert.assertEquals(true, result);

        result = filter2.filter("s2.t1");
        Assert.assertEquals(true, result);

        AviaterRegexFilter filter3 = new AviaterRegexFilter("foooo,f.*t");

        result = filter3.filter("fooooot");
        Assert.assertEquals(true, result);

        AviaterRegexFilter filter4 = new AviaterRegexFilter("otter2.otter_stability1|otter1.otter_stability1|retl.retl_mark|retl.retl_buffer|retl.xdual");
        result = filter4.filter("otter1.otter_stability1");
        Assert.assertEquals(true, result);
    }

    @Test
    public void test_regex_with_quantifier_comma() {
        // fix issue #5442: comma inside {n,m} quantifier should not split the pattern
        AviaterRegexFilter filter = new AviaterRegexFilter("test\\.order_\\d{1,2}");
        Assert.assertTrue(filter.filter("test.order_5"));
        Assert.assertTrue(filter.filter("test.order_42"));
        Assert.assertFalse(filter.filter("test.order_123"));
        Assert.assertFalse(filter.filter("test.order_"));

        // multiple patterns combined with quantifier should still split correctly
        AviaterRegexFilter filter2 = new AviaterRegexFilter("test\\.order_\\d{1,2},test\\.user_\\d+");
        Assert.assertTrue(filter2.filter("test.order_7"));
        Assert.assertTrue(filter2.filter("test.user_99"));
        Assert.assertFalse(filter2.filter("test.order_123"));

        // open-ended quantifier {n,}
        AviaterRegexFilter filter3 = new AviaterRegexFilter("test\\.t_\\d{2,}");
        Assert.assertTrue(filter3.filter("test.t_12"));
        Assert.assertTrue(filter3.filter("test.t_123"));
        Assert.assertFalse(filter3.filter("test.t_1"));
    }

    @Test
    public void testDisordered() {
        AviaterRegexFilter filter = new AviaterRegexFilter("u\\..*,uvw\\..*,uv\\..*,a\\.x,a\\.xyz,a\\.xy,abc\\.x,abc\\.xyz,abc\\.xy,ab\\.x,ab\\.xyz,ab\\.xy");

        boolean result = filter.filter("u.abc");
        Assert.assertEquals(true, result);

        result = filter.filter("ab.x");
        Assert.assertEquals(true, result);

        result = filter.filter("ab.xyz1");
        Assert.assertEquals(false, result);

        result = filter.filter("abc.xyz");
        Assert.assertEquals(true, result);

        result = filter.filter("uv.xyz");
        Assert.assertEquals(true, result);

    }

    @Test
    public void test_el() {
        AviaterELFilter filter = new AviaterELFilter("str(entry.entryType) == 'ROWDATA'");

        CanalEntry.Entry.Builder entry = CanalEntry.Entry.newBuilder();
        entry.setEntryType(CanalEntry.EntryType.ROWDATA);

        boolean result = filter.filter(entry.build());
        Assert.assertEquals(true, result);
    }
}

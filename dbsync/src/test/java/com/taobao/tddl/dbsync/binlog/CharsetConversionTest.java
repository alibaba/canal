package com.taobao.tddl.dbsync.binlog;

import java.nio.charset.Charset;

import org.junit.Assert;
import org.junit.Test;

/**
 * Regression for issue #5601: MariaDB 11.8 / recent MySQL versions emit collation
 * ids at or above the 2048-entry lookup-table ceiling. {@link CharsetConversion}
 * must degrade gracefully (warn + return null) for those ids instead of throwing,
 * because every caller already handles a null return.
 */
public class CharsetConversionTest {

    @Test
    public void getNioCharset_overflowId_returnsNullInsteadOfThrowing() {
        // id 2048 is the first id beyond the static lookup table ceiling (entries.length == 2048).
        // Before the fix this threw IllegalArgumentException("Invalid charset id: 2048"), aborting
        // binlog parsing. After the fix it degrades like any unknown id.
        Charset result = CharsetConversion.getNioCharset(2048);
        Assert.assertNull("charset id >= table size must not throw, must return null", result);
    }

    @Test
    public void getCharset_overflowId_returnsNullInsteadOfThrowing() {
        Assert.assertNull(CharsetConversion.getCharset(4096));
    }

    @Test
    public void getCollation_overflowId_returnsNullInsteadOfThrowing() {
        Assert.assertNull(CharsetConversion.getCollation(8192));
    }

    @Test
    public void getJavaCharset_overflowId_returnsNullInsteadOfThrowing() {
        Assert.assertNull(CharsetConversion.getJavaCharset(3000));
    }

    @Test
    public void negativeId_returnsNullInsteadOfThrowing() {
        // Defensive: negative ids are equally out of range and must not abort parsing either.
        Assert.assertNull(CharsetConversion.getNioCharset(-1));
    }

    @Test
    public void knownId_stillResolved() {
        // Regression guard: a well-known id (33 = utf8mb3_general_ci) must still resolve.
        Assert.assertEquals("utf8", CharsetConversion.getCharset(33));
    }
}

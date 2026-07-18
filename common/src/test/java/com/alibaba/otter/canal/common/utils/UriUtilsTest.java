package com.alibaba.otter.canal.common.utils;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.junit.Assert.*;

public class UriUtilsTest {

    @Test
    public void testParseQueryWithMultipleEqualsInValue() throws URISyntaxException {
        // When a query parameter value contains '=' (e.g., a=b=c),
        // the value should be "b=c", not truncated to "b".
        // This is the standard URL query parsing behavior (split on first '=' only).
        String uriString = "http://example.com?name=a=b=c";
        URI uri = new URI(uriString);

        Map<String, String> params = UriUtils.parseQuery(uri);

        assertTrue(params.containsKey("name"));
        assertEquals("a=b=c", params.get("name"));
    }

    @Test
    public void testParseQueryWithMultipleEqualsInValueFromString() throws URISyntaxException {
        // Same test using the String overload
        Map<String, String> params = UriUtils.parseQuery("http://example.com?token=x=y=z&w=1");

        assertEquals("x=y=z", params.get("token"));
        assertEquals("1", params.get("w"));
    }

    @Test
    public void testParseQueryNoEqualsSign() throws URISyntaxException {
        // A parameter with no '=' should have null value
        Map<String, String> params = UriUtils.parseQuery("http://example.com?flag");

        assertTrue(params.containsKey("flag"));
        assertNull(params.get("flag"));
    }

    @Test
    public void testParseQuerySimpleParameter() throws URISyntaxException {
        Map<String, String> params = UriUtils.parseQuery("http://example.com?foo=bar");

        assertEquals("bar", params.get("foo"));
    }
}

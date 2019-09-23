package com.alibaba.otter.canal.adapter.es.core.util;

import java.util.Locale;

public class StringUtils {
    private static final int TO_UPPER_CACHE_LENGTH = 2 * 1024;
    private static final int TO_UPPER_CACHE_MAX_ENTRY_LENGTH = 64;
    private static final String[][] TO_UPPER_CACHE = new String[TO_UPPER_CACHE_LENGTH][];

    public static String trim(String s, boolean leading, boolean trailing,
                       String sp) {
        char space = sp == null || sp.isEmpty() ? ' ' : sp.charAt(0);
        int begin = 0, end = s.length();
        if (leading) {
            while (begin < end && s.charAt(begin) == space) {
                begin++;
            }
        }
        if (trailing) {
            while (end > begin && s.charAt(end - 1) == space) {
                end--;
            }
        }
        // substring() returns self if start == 0 && end == length()
        return s.substring(begin, end);
    }

    public static String replaceAll(String s, String before, String after) {
        int next = s.indexOf(before);
        if (next < 0 || before.isEmpty()) {
            return s;
        }
        StringBuilder buff = new StringBuilder(
                s.length() - before.length() + after.length());
        int index = 0;
        while (true) {
            buff.append(s, index, next).append(after);
            index = next + before.length();
            next = s.indexOf(before, index);
            if (next < 0) {
                buff.append(s, index, s.length());
                break;
            }
        }
        return buff.toString();
    }

    public static String pad(String string, int n, String padding, boolean right) {
        if (n < 0) {
            n = 0;
        }
        if (n < string.length()) {
            return string.substring(0, n);
        } else if (n == string.length()) {
            return string;
        }
        char paddingChar;
        if (padding == null || padding.isEmpty()) {
            paddingChar = ' ';
        } else {
            paddingChar = padding.charAt(0);
        }
        StringBuilder buff = new StringBuilder(n);
        n -= string.length();
        if (right) {
            buff.append(string);
        }
        for (int i = 0; i < n; i++) {
            buff.append(paddingChar);
        }
        if (!right) {
            buff.append(string);
        }
        return buff.toString();
    }

    public static String toUpperEnglish(String s) {
        if (s.length() > TO_UPPER_CACHE_MAX_ENTRY_LENGTH) {
            return s.toUpperCase(Locale.ENGLISH);
        }
        int index = s.hashCode() & (TO_UPPER_CACHE_LENGTH - 1);
        String[] e = TO_UPPER_CACHE[index];
        if (e != null) {
            if (e[0].equals(s)) {
                return e[1];
            }
        }
        String s2 = s.toUpperCase(Locale.ENGLISH);
        e = new String[] { s, s2 };
        TO_UPPER_CACHE[index] = e;
        return s2;
    }
}

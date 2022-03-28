package com.alibaba.otter.canal.client.adapter.config.bind;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link PropertyNamePatternsMatcher} that matches when a property name exactly
 * matches one of the given names, or starts with one of the given names
 * followed by a delimiter. This implementation is optimized for frequent calls.
 *
 * @author Phillip Webb
 * @since 1.2.0
 */
class DefaultPropertyNamePatternsMatcher implements PropertyNamePatternsMatcher {

    private final char[]   delimiters;

    private final boolean  ignoreCase;

    private final String[] names;

    protected DefaultPropertyNamePatternsMatcher(char[] delimiters, String... names){
        this(delimiters, false, names);
    }

    protected DefaultPropertyNamePatternsMatcher(char[] delimiters, boolean ignoreCase, String... names){
        this(delimiters, ignoreCase, new HashSet<>(Arrays.asList(names)));
    }

    DefaultPropertyNamePatternsMatcher(char[] delimiters, boolean ignoreCase, Set<String> names){
        this.delimiters = delimiters;
        this.ignoreCase = ignoreCase;
        this.names = names.toArray(new String[names.size()]);
    }

    @Override
    public boolean matches(String propertyName) {
        char[] propertyNameChars = propertyName.toCharArray();
        boolean[] match = new boolean[this.names.length];
        boolean noneMatched = true;
        for (int i = 0; i < this.names.length; i++) {
            if (this.names[i].length() <= propertyNameChars.length) {
                match[i] = true;
                noneMatched = false;
            }
        }
        if (noneMatched) {
            return false;
        }
        for (int charIndex = 0; charIndex < propertyNameChars.length; charIndex++) {
            for (int nameIndex = 0; nameIndex < this.names.length; nameIndex++) {
                if (match[nameIndex]) {
                    match[nameIndex] = false;
                    if (charIndex < this.names[nameIndex].length()) {
                        if (isCharMatch(this.names[nameIndex].charAt(charIndex), propertyNameChars[charIndex])) {
                            match[nameIndex] = true;
                            noneMatched = false;
                        }
                    } else {
                        char charAfter = propertyNameChars[this.names[nameIndex].length()];
                        if (isDelimiter(charAfter)) {
                            match[nameIndex] = true;
                            noneMatched = false;
                        }
                    }
                }
            }
            if (noneMatched) {
                return false;
            }
        }
        for (boolean b : match) {
            if (b) {
                return true;
            }
        }
        return false;
    }

    private boolean isCharMatch(char c1, char c2) {
        if (this.ignoreCase) {
            return Character.toLowerCase(c1) == Character.toLowerCase(c2);
        }
        return c1 == c2;
    }

    private boolean isDelimiter(char c) {
        for (char delimiter : this.delimiters) {
            if (c == delimiter) {
                return true;
            }
        }
        return false;
    }

}

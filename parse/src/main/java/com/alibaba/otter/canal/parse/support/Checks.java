package com.alibaba.otter.canal.parse.support;

import java.util.Collection;

public abstract class Checks {
    public static void checkNotNull(Object target, String customMessage) {
        if (target == null) {
            throw new IllegalArgumentException(customMessage);
        }
    }

    public static void checkNotNull(Object target) {
        checkNotNull(target, "Argument can't be null.");
    }

    public static void checkNonEmptyArray(Object[] target, String customMsg) {
        if (target == null || target.length == 0) {
            throw new IllegalArgumentException(customMsg);
        }
    }

    public static void checkNonEmptyArray(Object[] target) {
        checkNonEmptyArray(target, "Empty or Null Array argument is not allowed.");
    }

    public static void checkNonEmptyArray(byte[] target, String customMsg) {
        if (target == null || target.length == 0) {
            throw new IllegalArgumentException(customMsg);
        }
    }

    public static void checkNonEmptyArray(byte[] target) {
        checkNonEmptyArray(target, "Empty or Null Array argument is not allowed.");
    }

    public static void checkNonEmptyCollection(Collection<?> target, String customMsg) {
        if (target == null || target.size() == 0) {
            throw new IllegalArgumentException(customMsg);
        }
    }

    public static void checkNonEmptyCollection(Collection<?> target) {
        checkNonEmptyCollection(target, "Empty or Null Collection argument is not allowed.");
    }

}

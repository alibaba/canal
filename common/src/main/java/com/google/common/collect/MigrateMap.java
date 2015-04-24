package com.google.common.collect;

import com.google.common.base.Function;
import java.util.concurrent.ConcurrentMap;

public class MigrateMap
{
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(MapMaker maker, Function<? super K, ? extends V> computingFunction)
    {
        return maker.makeComputingMap(computingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction)
    {
        return new MapMaker().makeComputingMap(computingFunction);
    }
}

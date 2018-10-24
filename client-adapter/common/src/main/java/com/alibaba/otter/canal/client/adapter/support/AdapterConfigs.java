package com.alibaba.otter.canal.client.adapter.support;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public interface AdapterConfigs {
    Multimap<String, String> configs = ArrayListMultimap.create();
}

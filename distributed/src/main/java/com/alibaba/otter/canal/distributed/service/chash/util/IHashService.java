package com.alibaba.otter.canal.distributed.service.chash.util;

/**
 * 一致性hash接口
 *
 * @author rewerma 2020-11-8 下午03:07:11
 * @version 1.0.0
 */
public interface IHashService {

    /**
     * 哈希方法
     * 
     * @param key 键
     * @return 哈希值
     */
    Long hash(String key);
}

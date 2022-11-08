package com.alibaba.otter.canal.parse.driver.mysql.packets;

import java.io.IOException;

/**
 * Created by hiwjd on 2018/4/23. hiwjd0@gmail.com
 */
public interface GTIDSet {

    /**
     * 序列化成字节数组
     *
     * @return
     */
    byte[] encode() throws IOException;

    /**
     * 更新当前实例
     * 
     * @param str
     * @throws Exception
     */
    void update(String str);
}

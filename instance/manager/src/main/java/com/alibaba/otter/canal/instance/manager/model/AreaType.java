package com.alibaba.otter.canal.instance.manager.model;

/**
 * 标记一下地区信息，因为不同地区会有不同的配置信息
 * 
 * @author jianghang 2013-6-5 下午04:14:05
 * @version 4.1.9
 */
public enum AreaType {

    HZ, US;

    public boolean isHzArea() {
        return this.equals(AreaType.HZ);
    }

    public boolean isUsArea() {
        return this.equals(AreaType.US);
    }
}

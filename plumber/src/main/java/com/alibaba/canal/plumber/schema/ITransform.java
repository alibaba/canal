package com.alibaba.canal.plumber.schema;

import com.alibaba.canal.plumber.model.EventData;

/**
 * @author dsqin
 * @date 2018/6/5
 */
public interface ITransform {

    public boolean transform(EventData eventData);
}

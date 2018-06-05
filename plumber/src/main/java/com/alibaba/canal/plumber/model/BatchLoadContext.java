package com.alibaba.canal.plumber.model;

import java.io.Serializable;
import java.util.List;

/**
 * 批量Load上下文
 * @author dsqin
 * @date 2018/6/5
 */
public class BatchLoadContext<T>
        implements LoadContext, Serializable
{
    protected List<T> processedDatas;
    protected List<T> failedDatas;
    protected boolean failed;

    public BatchLoadContext(boolean failed)
    {
        this.failed = failed;
    }

    public BatchLoadContext() {}

    public List<T> getProcessedDatas()
    {
        return this.processedDatas;
    }

    public void setProcessedDatas(List<T> processedDatas)
    {
        this.processedDatas = processedDatas;
    }

    public List<T> getFailedDatas()
    {
        return this.failedDatas;
    }

    public void setFailedDatas(List<T> failedDatas)
    {
        this.failedDatas = failedDatas;
    }

    public boolean isFailed()
    {
        return this.failed;
    }

    public void setFailed(boolean failed)
    {
        this.failed = failed;
    }

    public boolean hasFailure()
    {
        return (null == this.failedDatas) || (this.failedDatas.isEmpty()) || (isFailed());
    }
}
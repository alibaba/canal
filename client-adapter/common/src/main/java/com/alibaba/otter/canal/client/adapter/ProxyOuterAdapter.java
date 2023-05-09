package com.alibaba.otter.canal.client.adapter;

import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProxyOuterAdapter implements OuterAdapter {

    private OuterAdapter outerAdapter;

    public ProxyOuterAdapter(OuterAdapter outerAdapter) {
        this.outerAdapter = outerAdapter;
    }

    private ClassLoader changeCL() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(outerAdapter.getClass().getClassLoader());
        return cl;
    }

    private void revertCL(ClassLoader cl) {
        Thread.currentThread().setContextClassLoader(cl);
    }

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        ClassLoader cl = changeCL();
        try {
            outerAdapter.init(configuration, envProperties);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        ClassLoader cl = changeCL();
        try {
            outerAdapter.sync(dmls);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public void destroy() {
        ClassLoader cl = changeCL();
        try {
            outerAdapter.destroy();
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        ClassLoader cl = changeCL();
        try {
            return OuterAdapter.super.etl(task, params);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public Map<String, Object> count(String task) {
        ClassLoader cl = changeCL();
        try {
            return OuterAdapter.super.count(task);
        } finally {
            revertCL(cl);
        }
    }

    @Override
    public String getDestination(String task) {
        ClassLoader cl = changeCL();
        try {
            return OuterAdapter.super.getDestination(task);
        } finally {
            revertCL(cl);
        }
    }
}

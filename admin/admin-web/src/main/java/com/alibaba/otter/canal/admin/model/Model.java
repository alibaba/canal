package com.alibaba.otter.canal.admin.model;

import javax.persistence.MappedSuperclass;
import javax.persistence.OptimisticLockException;

import org.apache.commons.beanutils.PropertyUtils;

import io.ebean.Ebean;
import io.ebean.EbeanServer;

/**
 * EBean Model扩展类
 *
 * @author rewerma 2019-07-13 下午05:12:16
 * @version 1.0.0
 */
@MappedSuperclass
public abstract class Model extends io.ebean.Model {

    public void init() {
    }

    public void save() {
        init();
        super.save();
    }

    public void insert() {
        init();
        super.insert();
    }

    public void saveOrUpdate() {
        try {
            EbeanServer ebeanServer = Ebean.getDefaultServer();
            Object id = ebeanServer.getBeanId(this);
            if (id == null) {
                init();
                this.save();
            } else {
                this.update();
            }
        } catch (Exception e) {
            throw new OptimisticLockException(e);
        }
    }

    public void update(String... propertiesNames) {
        try {
            EbeanServer ebeanServer = Ebean.getDefaultServer();
            Object id = ebeanServer.getBeanId(this);
            if (id == null) {
                return;
            }
            Object model = ebeanServer.createQuery(this.getClass()).where().idEq(id).findOne();
            if (model == null) {
                return;
            }
            for (String propertyName : propertiesNames) {
                if (propertyName.startsWith("nn:")) { // not null
                    propertyName = propertyName.substring(3);
                    Object val = PropertyUtils.getProperty(this, propertyName);
                    if (val != null) {
                        PropertyUtils.setProperty(model, propertyName, val);
                    }
                } else {
                    Object val = PropertyUtils.getProperty(this, propertyName);
                    PropertyUtils.setProperty(model, propertyName, val);
                }
            }
            ebeanServer.update(model);
        } catch (Exception e) {
            throw new OptimisticLockException(e);
        }
    }
}

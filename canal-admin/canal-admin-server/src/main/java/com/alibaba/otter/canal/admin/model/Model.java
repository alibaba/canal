package com.alibaba.otter.canal.admin.model;

import io.ebean.Ebean;
import io.ebean.EbeanServer;
import org.apache.commons.beanutils.PropertyUtils;

import javax.persistence.MappedSuperclass;
import javax.persistence.OptimisticLockException;

@MappedSuperclass
public abstract class Model extends io.ebean.Model {

    public void update(String... propertiesNames) {
        try {
            EbeanServer ebeanServer = Ebean.getDefaultServer();
            Object id = ebeanServer.getBeanId(this);
            Object model = ebeanServer.createQuery(this.getClass()).where().idEq(id).findOne();
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

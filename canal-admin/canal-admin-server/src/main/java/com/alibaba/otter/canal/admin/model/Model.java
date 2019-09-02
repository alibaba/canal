package com.alibaba.otter.canal.admin.model;

import io.ebean.Ebean;
import io.ebean.EbeanServer;

import java.lang.reflect.Field;

import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.OptimisticLockException;

import org.apache.commons.beanutils.PropertyUtils;

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
            Field idField = null;
            // find id field
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                Id idAnn = field.getAnnotation(Id.class);
                if (idAnn != null) {
                    idField = field;
                    break;
                }
            }
            if (idField == null) {
                return;
            }
            Object idVal = PropertyUtils.getProperty(this, idField.getName());
            if (idVal == null) {
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

package com.alibaba.otter.canal.deployer;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 启动的相关配置
 * 
 * @author jianghang 2012-11-8 下午02:50:54
 * @version 1.0.0
 */
public class InstanceConfig {

    private InstanceConfig globalConfig;
    private InstanceMode   mode;
    private Boolean        lazy;
    private String         managerAddress;
    private String         springRootXml;
    private String         springCustomXml;

    public InstanceConfig(){

    }

    public InstanceConfig(InstanceConfig globalConfig){
        this.globalConfig = globalConfig;
    }

    public static enum InstanceMode {
        SPRING, MANAGER;

        public boolean isSpring() {
            return this == InstanceMode.SPRING;
        }

        public boolean isManager() {
            return this == InstanceMode.MANAGER;
        }
    }

    public Boolean getLazy() {
        if (lazy == null && globalConfig != null) {
            return globalConfig.getLazy();
        } else {
            return lazy;
        }
    }

    public void setLazy(Boolean lazy) {
        this.lazy = lazy;
    }

    public InstanceMode getMode() {
        if (mode == null && globalConfig != null) {
            return globalConfig.getMode();
        } else {
            return mode;
        }
    }

    public void setMode(InstanceMode mode) {
        this.mode = mode;
    }

    public String getManagerAddress() {
        if (managerAddress == null && globalConfig != null) {
            return globalConfig.getManagerAddress();
        } else {
            return managerAddress;
        }
    }

    public void setManagerAddress(String managerAddress) {
        this.managerAddress = managerAddress;
    }

    public String getSpringRootXml() {
        if (springRootXml == null && globalConfig != null) {
            return globalConfig.getSpringRootXml();
        } else {
            return springRootXml;
        }
    }

    public void setSpringRootXml(String springRootXml) {
        this.springRootXml = springRootXml;
    }

    public String getSpringCustomXml() {
        if (springCustomXml == null && globalConfig != null) {
            return globalConfig.getSpringCustomXml();
        } else {
            return springCustomXml;
        }
    }

    public void setSpringCustomXml(String springCustomXml) {
        this.springCustomXml = springCustomXml;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}

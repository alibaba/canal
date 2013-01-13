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
    private String         springRootDir;
    private String         springCustomDir;

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

    public String getSpringRootDir() {
        if (springRootDir == null && globalConfig != null) {
            return globalConfig.getSpringRootDir();
        } else {
            return springRootDir;
        }
    }

    public void setSpringRootDir(String springRootDir) {
        this.springRootDir = springRootDir;
    }

    public String getSpringCustomDir() {
        if (springCustomDir == null && globalConfig != null) {
            return globalConfig.getSpringCustomDir();
        } else {
            return springCustomDir;
        }
    }

    public void setSpringCustomDir(String springCustomDir) {
        this.springCustomDir = springCustomDir;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}

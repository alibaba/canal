package com.alibaba.otter.canal.deployer;

import java.text.MessageFormat;

/**
 * 启动常用变量
 * 
 * @author jianghang 2012-11-8 下午03:15:55
 * @version 1.0.0
 */
public class CanalConstants {

    public static final String MDC_DESTINATION                   = "destination";
    public static final String ROOT                              = "canal";
    public static final String CANAL_ID                          = ROOT + "." + "id";
    public static final String CANAL_IP                          = ROOT + "." + "ip";
    public static final String CANAL_PORT                        = ROOT + "." + "port";
    public static final String CANAL_ZKSERVERS                   = ROOT + "." + "zkServers";

    public static final String CANAL_DESTINATIONS                = ROOT + "." + "destinations";
    public static final String CANAL_AUTO_SCAN                   = ROOT + "." + "auto.scan";
    public static final String CANAL_AUTO_SCAN_INTERVAL          = ROOT + "." + "auto.scan.interval";
    public static final String CANAL_CONF_DIR                    = ROOT + "." + "conf.dir";

    public static final String CANAL_DESTINATION_SPLIT           = ",";
    public static final String GLOBAL_NAME                       = "global";

    public static final String INSTANCE_MODE_TEMPLATE            = ROOT + "." + "instance.{0}.mode";
    public static final String INSTANCE_LAZY_TEMPLATE            = ROOT + "." + "instance.{0}.lazy";
    public static final String INSTANCE_MANAGER_ADDRESS_TEMPLATE = ROOT + "." + "instance.{0}.manager.address";
    public static final String INSTANCE_SPRING_XML_TEMPLATE      = ROOT + "." + "instance.{0}.spring.xml";

    public static final String CANAL_DESTINATION_PROPERTY        = ROOT + ".instance.destination";

    public static String getInstanceModeKey(String destination) {
        return MessageFormat.format(INSTANCE_MODE_TEMPLATE, destination);
    }

    public static String getInstanceManagerAddressKey(String destination) {
        return MessageFormat.format(INSTANCE_MANAGER_ADDRESS_TEMPLATE, destination);
    }

    public static String getInstancSpringXmlKey(String destination) {
        return MessageFormat.format(INSTANCE_SPRING_XML_TEMPLATE, destination);
    }

    public static String getInstancLazyKey(String destination) {
        return MessageFormat.format(INSTANCE_LAZY_TEMPLATE, destination);
    }
}

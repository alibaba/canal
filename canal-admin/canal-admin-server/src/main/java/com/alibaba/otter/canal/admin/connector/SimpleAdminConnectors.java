package com.alibaba.otter.canal.admin.connector;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleAdminConnectors {

    private static final Logger logger        = LoggerFactory.getLogger(SimpleAdminConnectors.class);
    private static String       defaultUser   = "admin";
    private static String       defaultPasswd = "admin";

    public static <R> R execute(String ip, int port, Function<AdminConnector, R> function) {
        return execute(ip, port, defaultUser, defaultPasswd, function);
    }

    public static <R> R execute(String ip, int port, String user, String passwd, Function<AdminConnector, R> function) {
        SimpleAdminConnector connector = new SimpleAdminConnector(ip, port, user, passwd);
        try {
            connector.connect();
            return function.apply(connector);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            connector.disconnect();
        }

        return null;
    }
}

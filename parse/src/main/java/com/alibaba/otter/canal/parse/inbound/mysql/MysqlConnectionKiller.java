package com.alibaba.otter.canal.parse.inbound.mysql;

import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.OKPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MysqlConnectionKiller {

    private static Logger logger = LoggerFactory.getLogger(MysqlConnectionKiller.class);

    private MysqlConnectionKiller() {}

    private static class HOLDER {
        static MysqlConnectionKiller INTANCE = new MysqlConnectionKiller();
    }

    public static MysqlConnectionKiller getInstance() {
        return HOLDER.INTANCE;
    }

    private List<MysqlConnection> killList = new ArrayList<MysqlConnection>();

    public void kill() {
        for (MysqlConnection connection : killList) {
            MysqlConnector connector = connection.getConnector().fork();
            try {
                connector.connect();
                MysqlUpdateExecutor executor = new MysqlUpdateExecutor(connector);
                OKPacket okPacket = executor.update("KILL CONNECTION " + connector.getConnectionId());
                if (!okPacket.getMessage().contains("Error")) {
                    logger.warn("Kill alive mysql connection");
                }
            } catch (IOException e) {
                logger.debug("kill mysql connection failed");
            } finally {
                if (connector != null) {
                    try {
                        connector.disconnect();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            }
        }
        killList.clear();
    }

    public void addConnectionToKill(MysqlConnection connectionToKill) {
        killList.add(connectionToKill);
        if (killList.size() > 100) {
            logger.warn("Kill list size is: " + killList.size());
        }
    }
}

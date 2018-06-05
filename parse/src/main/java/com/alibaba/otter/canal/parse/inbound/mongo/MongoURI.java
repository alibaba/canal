package com.alibaba.otter.canal.parse.inbound.mongo;

import com.google.common.collect.Lists;
import com.mongodb.ServerAddress;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * mongo uri解析
 * @author dsqin
 * @date 2018/5/16
 */
public class MongoURI {

    private final static String MONGO_SCHEMA = "mongo";

    /**
     * 解析uri mongo://172.31.1.174:27017%3F172.31.1.174:27017
     * @param uriString
     * @return
     * @throws URISyntaxException
     * @throws UnknownHostException
     */
    public static List<ServerAddress> parseURI(String uriString) throws URISyntaxException, UnknownHostException {
        URI uri = new URI(uriString);

        String scheme = uri.getScheme();

        if (!MONGO_SCHEMA.equals(scheme)) {
            return null;
        }

        String authority = uri.getAuthority();

        String[] authoritys = StringUtils.splitByWholeSeparatorPreserveAllTokens(authority, "/");

        int authoritySize = authoritys.length;

        List<ServerAddress> serverAddressList = Lists.newArrayListWithCapacity(authoritySize);

        for (int i = 0; i < authoritySize; i++) {
            String ipPortString = authoritys[i];
            String[] ipPorts = StringUtils.splitByWholeSeparatorPreserveAllTokens(ipPortString, ":");

            if (ArrayUtils.isEmpty(ipPorts)) {
                continue;
            }

            String ip = ipPorts[0];
            int port = NumberUtils.toInt(ipPorts[1], 27017);
            ServerAddress serverAddress = new ServerAddress(ip, port);
            serverAddressList.add(serverAddress);

        }

        return serverAddressList;
    }
}

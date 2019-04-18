package com.alibaba.otter.canal.client.adapter.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.*;


/**
 * Redis 适配器实现类
 *
 * @author ivothgle 2019-04-18
 * @version 1.0.0
 */
@SPI("redis")
public class RedisAdapter implements OuterAdapter {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean isCluster;

    private Jedis jedis;
    private JedisCluster jedisCluster;
    private String redisKey;

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            Map<String, String> properties = configuration.getProperties();
            String cluster = properties.get("redis.cluster");
            isCluster = Boolean.parseBoolean(cluster);
            redisKey = properties.get("redis.key");

            String[] hostArray = configuration.getHosts().split(",");
            if (isCluster) {
                Set<HostAndPort> jedisClusterNodes = new HashSet<>();
                for (String host : hostArray) {
                    int i = host.indexOf(":");
                    jedisClusterNodes.add(new HostAndPort(host.substring(0, i), Integer.parseInt(host.substring(i + 1))));
                }
                jedisCluster = new JedisCluster(jedisClusterNodes);
            } else {
                String host = hostArray[0];
                int i = host.indexOf(":");
                jedis = new Jedis(host.substring(0, i), Integer.parseInt(host.substring(i + 1)));
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }

        for (Dml dml : dmls) {
            String json = JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue);

            Long ret;
            if (isCluster) {
                ret = this.jedisCluster.rpush(redisKey, json);
            } else {
                ret = this.jedis.rpush(redisKey, json);
            }
            logger.info("DML: {} ret: {}", json, ret);
        }
    }

    @Override
    public void destroy() {
        if (jedis != null) {
            jedis.close();
        }

        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }
}

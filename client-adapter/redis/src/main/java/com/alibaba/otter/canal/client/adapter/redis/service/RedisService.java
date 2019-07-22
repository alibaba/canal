package com.alibaba.otter.canal.client.adapter.redis.service;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class RedisService {
    private boolean isCluster;
    private Object redis;

    public RedisService(String hosts, boolean isCluster) {
        String[] hostArray = hosts.split(",");
        this.isCluster = isCluster;

        if (isCluster) {
            Set<HostAndPort> jedisClusterNodes = new HashSet<>();
            for (String host : hostArray) {
                int i = host.indexOf(":");
                jedisClusterNodes.add(new HostAndPort(host.substring(0, i), Integer.parseInt(host.substring(i + 1))));
            }
            redis = new JedisCluster(jedisClusterNodes);
        } else {
            String host = hostArray[0];
            int i = host.indexOf(":");
            redis = new Jedis(host.substring(0, i), Integer.parseInt(host.substring(i + 1)));
        }
    }

    public JedisCluster getCluster() {
        return (JedisCluster) redis;
    }

    public Jedis getSingle() {
        return (Jedis) redis;
    }


    public void close() {
        if (redis != null) {
            if (isCluster) {
                getCluster().close();
            } else {
                getSingle().close();
            }
        }
    }

    public String set(byte[] key, int expire, byte[] value) {
        if (isCluster) {
            if (expire > 0) {
                return getCluster().setex(key, expire, value);
            } else {
                return getCluster().set(key, value);
            }
        } else {
            if (expire > 0) {
                return getSingle().setex(key, expire, value);
            } else {
                return getSingle().set(key, value);
            }
        }
    }

    public Long del(String keys) {
        if (isCluster) {
            return getCluster().del(keys);
        } else {
            return getSingle().del(keys);
        }
    }
}

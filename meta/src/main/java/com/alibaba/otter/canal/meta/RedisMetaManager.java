package com.alibaba.otter.canal.meta;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.meta.exception.CanalMetaManagerException;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class RedisMetaManager extends AbstractCanalLifeCycle implements CanalMetaManager {

    private static final String  KEY_SEPARATOR="_";

    private static final String KEY_ROOT="otter"+KEY_SEPARATOR+"canal"+KEY_SEPARATOR+"meta";

    private static final String KEY_DESTINATIONS="destination";

    private static final String KEY_CURSOR="cursor";

    private static final String KEY_BATCH="batch";

    private static final String KEY_MAX="max";

    private JedisPool jedisPool;

    private JedisPoolConfig jedisPoolConfig;

    private String redisHost;

    private Integer redisPort;

    @Override
    public void start() {
        super.start();
        if(jedisPoolConfig==null){
            jedisPool=new JedisPool(redisHost,redisPort);
        }else{
            jedisPool=new JedisPool(jedisPoolConfig,redisHost,redisPort);
        }

    }

    @Override
    public void stop() {
        super.stop();
        jedisPool.close();
    }

    @Override
    public void subscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        invokRedis(jedis -> jedis.hset(getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination()),String.valueOf(clientIdentity.getClientId()), JsonUtils.marshalToString(clientIdentity)));
    }

    @Override
    public boolean hasSubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        return invokRedis(jedis -> jedis.hexists(getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination()),String.valueOf(clientIdentity.getClientId())));
    }

    @Override
    public void unsubscribe(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        invokRedis(jedis -> jedis.hdel(getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination()),String.valueOf(clientIdentity.getClientId())));
    }

    @Override
    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        String json= invokRedis(jedis -> jedis.get(getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination(),clientIdentity.getClientId(),KEY_CURSOR)));
        if(StringUtils.isNotEmpty(json)){
            return JsonUtils.unmarshalFromString(json,Position.class);
        }
        return null;
    }

    @Override
    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        invokRedis(jedis -> jedis.set(getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination(),clientIdentity.getClientId(),KEY_CURSOR),JsonUtils.marshalToString(position,SerializerFeature.WriteClassName)));
    }

    @Override
    public List<ClientIdentity> listAllSubscribeInfo(String destination) throws CanalMetaManagerException {
        List<String> jsons= invokRedis(jedis -> jedis.hvals(getRedisKey(KEY_DESTINATIONS,destination)));
        List<ClientIdentity> results=new ArrayList<>();
        for(String json:jsons){
            results.add(JsonUtils.unmarshalFromString(json,ClientIdentity.class));
        }
        return results;
    }

    @Override
    public PositionRange getFirstBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Set<String> jsons=invokRedis(jedis -> jedis.zrange(getKeyOfClientBatch(clientIdentity),0,0));
        if(jsons==null||jsons.isEmpty()){
            return null;
        }
        String json=jsons.iterator().next();
        return JsonUtils.unmarshalFromString(json,PositionRange.class);
    }

    @Override
    public PositionRange getLastestBatch(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Set<String> jsons=invokRedis(jedis -> jedis.zrange(getKeyOfClientBatch(clientIdentity),-1,-1));
        if(jsons==null||jsons.isEmpty()){
            return null;
        }
        String json=jsons.iterator().next();
        return JsonUtils.unmarshalFromString(json,PositionRange.class);
    }

    @Override
    public Long addBatch(ClientIdentity clientIdentity, PositionRange positionRange) throws CanalMetaManagerException {
        Long batchId= invokRedis(jedis ->  jedis.incr(getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination(),clientIdentity.getClientId(),KEY_MAX,KEY_BATCH)));
        addBatch(clientIdentity,positionRange,batchId);
        return batchId;
    }

    @Override
    public void addBatch(ClientIdentity clientIdentity, PositionRange positionRange, Long batchId) throws CanalMetaManagerException {
        String key=getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination(),clientIdentity.getClientId(),KEY_MAX,KEY_BATCH);
        invokRedis(jedis -> jedis.zadd(getKeyOfClientBatch(clientIdentity),
                batchId.doubleValue(),JsonUtils.marshalToString(positionRange, SerializerFeature.WriteClassName)));
        String maxBatchId= invokRedis(jedis -> jedis.get(key));
        if(Long.parseLong(maxBatchId)<batchId){
            invokRedis(jedis -> jedis.set(key,String.valueOf(batchId)));
        }
    }

    @Override
    public PositionRange getBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        Set<String> jsons= invokRedis(jedis -> jedis.zrangeByScore(getKeyOfClientBatch(clientIdentity),batchId.doubleValue(),batchId.doubleValue()));
        if(jsons==null||jsons.isEmpty()){
            return null;
        }
        String json=jsons.iterator().next();
        return JsonUtils.unmarshalFromString(json,PositionRange.class);
    }

    @Override
    public PositionRange removeBatch(ClientIdentity clientIdentity, Long batchId) throws CanalMetaManagerException {
        Set<Tuple> tuples= invokRedis(jedis -> jedis.zrangeWithScores(getKeyOfClientBatch(clientIdentity),0,0));
        if(tuples==null||tuples.isEmpty()){
            return null;
        }
        Long minBatchId=new Double(tuples.iterator().next().getScore()).longValue();
        if (!minBatchId.equals(batchId)) {
            // 检查一下提交的ack/rollback，必须按batchId分出去的顺序提交，否则容易出现丢数据
            throw new CanalMetaManagerException(String.format("batchId:%d is not the firstly:%d", batchId, minBatchId));
        }
        PositionRange positionRange= getBatch(clientIdentity,batchId);
        if(positionRange==null){
            return  null;
        }
        invokRedis(jedis -> jedis.zremrangeByScore(getKeyOfClientBatch(clientIdentity),batchId,batchId));
        return positionRange;
    }

    @Override
    public Map<Long, PositionRange> listAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Map<Long, PositionRange> positionRanges = Maps.newLinkedHashMap();
        Set<Tuple> tuples= invokRedis(jedis -> jedis.zrangeWithScores(getKeyOfClientBatch(clientIdentity),0,-1));
        if(tuples==null||tuples.isEmpty()){
            return positionRanges;
        }
        for(Tuple tuple:tuples){
            Long batchId=new Double(tuple.getScore()).longValue();
            positionRanges.put(batchId,JsonUtils.unmarshalFromString(tuple.getElement(),PositionRange.class));
        }
        return positionRanges;
    }

    @Override
    public void clearAllBatchs(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        invokRedis(jedis -> jedis.zremrangeByRank(getKeyOfClientBatch(clientIdentity),0,-1));
    }

    private String getKeyOfClientBatch(ClientIdentity clientIdentity){
        return getRedisKey(KEY_DESTINATIONS,clientIdentity.getDestination(),clientIdentity.getClientId(),clientIdentity,KEY_BATCH);
    }

    private String getRedisKey(Object... args){
        StringBuilder stringBuilder=new StringBuilder(KEY_ROOT);
        for(Object obj:args){
            stringBuilder.append(KEY_SEPARATOR);
            stringBuilder.append(obj);
        }
        return stringBuilder.toString();
    }

    private <V> V invokRedis(Function<Jedis,V> function){
        Jedis jedis=null;
        try{
            jedis=jedisPool.getResource();
            return function.apply(jedis);
        }catch (Throwable t){
            return null;
        }finally {
            if(jedis!=null){
                jedis.close();
            }
        }
    }

    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public void setRedisPort(Integer redisPort) {
        this.redisPort = redisPort;
    }
}
